/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.almightshell.efiles;

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileSystemException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_KEY;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.IOUtils;

/**
 *
 * @author Shell
 */
public class EFile {

    private static final Log LOG = LogFactory.getLog(EFile.class);
    private static final String INDEX_NAME = "index-";
    private static final String PART_NAME = "part-";
    private static final String METADATA_NAME = "metadata";

    /**
     * size of each part file size *
     */
    long partMaxSize = 2 * 1024 * 1024 * 1024l;
    /**
     * size of blocks in hadoop archives *
     */
    long blockSize = 512 * 1024 * 1024l;
    long indexBlockSize = 10 * 1024 * 1024l;

    private final Configuration conf;
    private Path filePath = null;
    private Path metadataPath = null;
    private FileSystem fs = null;

    EFileMetadata metadata = new EFileMetadata();
    private Path currentDataPartPath = null;

    private EFile(Configuration conf, Path filePath, boolean newFile) throws IOException {
        this.conf = conf;
        this.filePath = filePath;
        fs = FileSystem.get(conf);

        if (newFile) {
            if (fs.exists(filePath)) {
                throw new FileAlreadyExistsException("The file " + filePath.getName() + " already exists");
            }
            fs.mkdirs(filePath);

            metadata.getDirectory().init(newBucket());
            this.currentDataPartPath = newPartFilePath();
            metadata.setCurrentDataPartPath(currentDataPartPath.getName());
            writeMetadata();
        } else {
            if (!fs.exists(filePath)) {
                throw new FileNotFoundException("The file " + filePath.getName() + " not found");
            }
            readMetadata();
            currentDataPartPath = new Path(filePath, metadata.getCurrentDataPartPath());
        }
    }

    public static EFile newFile(Configuration conf, Path filePath) throws IOException {
        return new EFile(conf, filePath, true);
    }

    public static EFile open(Configuration conf, Path filePath) throws IOException {
        return new EFile(conf, filePath, false);
    }

    private void put(FSDataOutputStream out, Path path) throws IOException {

        //Build index record
        BucketEntry entry = new BucketEntry();
        entry.setFileName(path.getName());
        entry.setSize((int) fs.getFileStatus(path).getLen());
        entry.setParteFileName(currentDataPartPath.getName());
        entry.setOffset((int) out.getPos());

        //
        if (entry.getSize() > blockSize) {
            throw new FileSystemException(path.getName() + " is not a small. The file size is too big");
        }

        //copy the file content
        try (FSDataInputStream in = fs.open(path)) {
            IOUtils.copyBytes(in, out, conf, false);
        }
        addBucketEntry(entry);
    }

    public void put(Path path) throws IOException {
        if (fs.getUsed(currentDataPartPath) >= partMaxSize) {
            currentDataPartPath = newPartFilePath();
            metadata.setCurrentDataPartPath(currentDataPartPath.getName());
        }

        try (FSDataOutputStream out = fs.append(currentDataPartPath)) {
            put(out, path);
        }
        writeMetadata();
    }

    public void putAllFilesFromDir(Path dirpath, boolean recursive) throws IOException {
        RemoteIterator<LocatedFileStatus> it = fs.listFiles(dirpath, recursive);

        FSDataOutputStream out = fs.append(currentDataPartPath);

        while (it.hasNext()) {
            //check if the used part file reaches the max size
            if (fs.getUsed(currentDataPartPath) >= partMaxSize) {
                currentDataPartPath = newPartFilePath();
                metadata.setCurrentDataPartPath(currentDataPartPath.getName());

                out.close();
                out = fs.append(currentDataPartPath);
            }

            LocatedFileStatus lfs = it.next();

            if (lfs.isFile()) {
                put(out, lfs.getPath());
            }
        }
        out.close();
        writeMetadata();
    }

    public List<BucketEntry> listFiles() throws IOException {

        List<BucketEntry> files = new ArrayList<>();
        metadata.getDirectory().getBuckets().stream().forEach(bucket -> {
            try (FSDataInputStream in = fs.open(bucket.getPath())) {

                while (in.available() > 0) {
                    BucketEntry entry = new BucketEntry();
                    entry.readFields(in);
                    files.add(entry);
                }
            } catch (IOException ex) {
                Logger.getLogger(EFile.class.getName()).log(Level.SEVERE, null, ex);
            }
        });

        return files;
    }

    public InputStream get(String name) throws IOException {
        BucketEntry entry = getBucketEntry(name);

        if (entry == null) {
            throw new FileNotFoundException(name + " not found in " + filePath.getName());
        }

        return get(entry);
    }

    public InputStream get(BucketEntry entry) throws IOException {
        byte[] b = new byte[entry.getSize()];
        try (FSDataInputStream in = fs.open(new Path(filePath, entry.getParteFileName()))) {
            in.seek(entry.getOffset());
            in.read(b, 0, entry.getSize());
        }

        return new ByteArrayInputStream(b);
    }

    private void addBucketEntry(BucketEntry entry) throws IOException {
        int key = entry.hashCode();
        int position = metadata.getDirectory().positionInDirectory(key);
        Bucket bucket = metadata.getDirectory().getBucket(position);

        //write the index record
        try (FSDataOutputStream out = fs.append(bucket.getPath())) {
            entry.write(out);
            bucket.setSize(bucket.getSize() + 1);
        }

        //split the bucket if full
        if (bucket.getSize() >= metadata.getBucketCapacity()) {
            splitBucket(bucket, key);
        }

    }

    private BucketEntry getBucketEntry(String name) throws IOException {

        int position = metadata.getDirectory().positionInDirectory(name.hashCode());
        Bucket bucket = metadata.getDirectory().getBucket(position);

        //read the index record
        try (FSDataInputStream in = fs.open(bucket.getPath())) {
            BucketEntry entry = new BucketEntry();
            while (in.available() > 0) {
                entry.readFields(in);
                if (entry.getFileName().equals(name)) {
                    return entry;
                }
            }
        }
        return null;
    }

    private void splitBucket(Bucket toSplitBucket, int key) throws IOException {
        Bucket newBucket = newBucket();

        if (toSplitBucket.getLocalDepth() == metadata.getDirectory().getGlobalDepth()) {
            metadata.getDirectory().doubleSize();
        }
        toSplitBucket.setLocalDepth(metadata.getDirectory().getGlobalDepth());
        newBucket.setLocalDepth(metadata.getDirectory().getGlobalDepth());

        //
        int[] poss = EFilesUtil.checkSplitPositionsInDirectory(key, metadata.getDirectory().getGlobalDepth());
        int pos1 = poss[0];
        int pos2 = poss[1];
        metadata.getDirectory().putBucket(toSplitBucket, pos1);
        metadata.getDirectory().putBucket(newBucket, pos2);

        List<BucketEntry> remainEntrys = new ArrayList<>();
        int p;
        try (FSDataOutputStream newOut = fs.append(newBucket.getPath()); FSDataInputStream in = fs.open(toSplitBucket.getPath())) {

            //redistribute data into the new bucket
            while (in.available() > 0) {
                BucketEntry entry = new BucketEntry();
                entry.readFields(in);

                p = metadata.getDirectory().positionInDirectory(entry.hashCode());
                if (p == pos2) {
                    entry.write(newOut);
                    newBucket.setSize(newBucket.getSize() + 1);
                } else {
                    remainEntrys.add(entry);
                }
            }
        }

        //update original bucket
        try (FSDataOutputStream out = fs.create(toSplitBucket.getPath(), true, conf.getInt(IO_FILE_BUFFER_SIZE_KEY, IO_FILE_BUFFER_SIZE_DEFAULT), metadata.getRepl(), indexBlockSize)) {
            toSplitBucket.setSize(0);
            for (BucketEntry e : remainEntrys) {
                e.write(out);
                toSplitBucket.setSize(toSplitBucket.getSize() + 1);
            }
        }

    }

    private Bucket newBucket() throws IOException {
        Bucket bucket = new Bucket();
        bucket.setLocalDepth(metadata.getDirectory().getGlobalDepth());
        bucket.setPath(new Path(filePath, INDEX_NAME + metadata.getIndexLabel()));

        fs.create(bucket.getPath(), false, conf.getInt(IO_FILE_BUFFER_SIZE_KEY, IO_FILE_BUFFER_SIZE_DEFAULT), metadata.getRepl(), indexBlockSize).close();

        metadata.setIndexLabel(metadata.getIndexLabel() + 1);
        return bucket;
    }

    private Path newPartFilePath() throws IOException {
        Path p = new Path(filePath, PART_NAME + metadata.getPartLabel());
        fs.create(p, false, conf.getInt(IO_FILE_BUFFER_SIZE_KEY, IO_FILE_BUFFER_SIZE_DEFAULT), metadata.getRepl(), blockSize).close();
        metadata.setPartLabel(metadata.getPartLabel() + 1);
        return p;
    }

    private void writeMetadata() throws IOException {
        try (FSDataOutputStream out = fs.create(getMetadataPath(), true, conf.getInt(IO_FILE_BUFFER_SIZE_KEY, IO_FILE_BUFFER_SIZE_DEFAULT), metadata.getRepl(), blockSize)) {
            metadata.write(out);
        }
    }

    private void readMetadata() throws IOException {
        if (!fs.exists(getMetadataPath())) {
            writeMetadata();
        } else {
            try (FSDataInputStream in = fs.open(getMetadataPath())) {
                metadata = new EFileMetadata();
                metadata.readFields(in);
            }
        }
    }

    private boolean checkValidName(String name) {
        Path tmp = new Path(name);
        if (tmp.depth() != 1) {
            return false;
        }
        if (name.endsWith(".elf")) {
            return true;
        }
        return false;
    }

    private Path getMetadataPath() {
        if (metadataPath == null) {
            metadataPath = new Path(filePath, METADATA_NAME);
        }
        return metadataPath;
    }

    public long getPartMaxSize() {
        return partMaxSize;
    }

    public void setPartMaxSize(long partMaxSize) {
        this.partMaxSize = partMaxSize;
    }

    public long getBlockSize() {
        return blockSize;
    }

    public void setBlockSize(long blockSize) {
        this.blockSize = blockSize;
    }

}
