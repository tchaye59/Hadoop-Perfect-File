/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.almightshell.pf;

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import static net.almightshell.pf.PerfectFile.PART_NAME;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.util.Shell.ShellCommandExecutor;

/**
 *
 * @author Shell
 */
public class Reader implements java.io.Closeable {

    private final Configuration conf;
    private final Path dirName;
    private final FileSystem fs;
    private final PerfectFileMetadata metadata;

    private Path metadataPath = null;

    public Reader(Configuration conf, Path dirName) throws IOException {
        this.conf = conf;
        this.dirName = dirName;

        this.fs = FileSystem.get(conf);

        this.metadata = new PerfectFileMetadata(fs);

        if (!fs.exists(dirName)) {
            throw new IOException("The file " + dirName + " is not found");
        }
        readMetadata();
        
        for (Bucket bucket : this.metadata.getDirectory().getBuckets()) {
              ShellCommandExecutor shexec = new ShellCommandExecutor(
               new String[] { "perl", "-e", "print 42" });
        }
    }

    public byte[] getBytes(String key) throws IOException {
        BucketEntry be = getEntryFromBucketByPerfectTable(PerfectFilesUtil.getHash(key));
        if (be == null) {
            throw new FileNotFoundException(key + " not found in " + dirName.getName());
        }
        return getBytes(be);
    }

    public InputStream get(String key) throws IOException {
        return new ByteArrayInputStream(getBytes(key));
    }

    public byte[] getBytes(BucketEntry entry) throws IOException {
        byte[] bs;
        int compressedDataSize;
        try (FSDataInputStream in = fs.open(getPartFilePath(entry.getPartFilePosition()))) {
            in.seek(entry.getOffset());

            //read lenght
            IntWritable iw = new IntWritable();
            iw.readFields(in);

            BytesWritable bw = new BytesWritable();
            bw.readFields(in);

            compressedDataSize = iw.get();
            bs = bw.getBytes();
        }
        return PerfectFilesUtil.decompress(bs, compressedDataSize);
    }

    private BucketEntry getEntryFromBucketByPerfectTable(long keyHash) throws IOException {
        Bucket bucket = metadata.getDirectory().getBucketByEntryKey(keyHash);
        //read the index record

        long pos = metadata.getPerfectTableHolder().get(bucket, keyHash);

        if (pos < 0) {
            return null;
        }
        long offSet = pos * BucketEntry.RECORD_SIZE;

        BucketEntry entry = new BucketEntry();
        
        try (FSDataInputStream in = fs.open(bucket.getPath())) {
            in.seek(offSet);
            entry.readFields(in);
        }

        return entry.getFileNameHash() != keyHash ? null : entry;
    }

    private List<BucketEntry> getBucketAllEntries(Bucket bucket) throws IOException {

        List<BucketEntry> bucketEntrys = new ArrayList<>();
        //read the index record
        try (FSDataInputStream in = fs.open(bucket.getPath())) {

            while (in.available() > 0) {
                BucketEntry entry = new BucketEntry();
                entry.readFields(in);

                bucketEntrys.add(entry);
            }
        }
        return bucketEntrys;
    }

    private void readMetadata() throws IOException {
        if (!fs.exists(getMetadataPath())) {
            throw new IOException("The file " + getMetadataPath() + " is not found");
        }
        try (FSDataInputStream in = fs.open(getMetadataPath())) {
            metadata.readFields(in);
        }
    }

    private Path getMetadataPath() {
        if (metadataPath == null) {
            metadataPath = new Path(dirName, PerfectFile.METADATA_NAME);
        }
        return metadataPath;
    }

    private Path getPartFilePath(int position) {
        return new Path(dirName, PART_NAME + position);
    }

    @Override
    public void close() throws IOException {

    }

}
