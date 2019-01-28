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
import java.nio.file.FileSystemException;
import java.util.ArrayList;
import java.util.LinkedList;
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
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

/**
 *
 * @author Shell
 */
public class PerfectFile {

    public static final Log LOG = LogFactory.getLog(PerfectFile.class);
    public static final String INDEX_NAME = "index-";
    public static final String NAMES = "names";
    public static final String TEMPORARY_INDEX_NAME = "temopraryIndex";
    public static final String PART_NAME = "part-";
    public static final String METADATA_NAME = "metadata";

    public static class Writer implements java.io.Closeable {

        private final Configuration conf;
        private final Path dirName;
        private Path metadataPath = null;

        /**
         * size of each part file size *
         */
        long partMaxSize = 5 * 1024 * 1024 * 1024l;

        /**
         * size of blocks in hadoop archives *
         */
        public static long blockSize = 512 * 1024 * 1024l;
        public static long indexBlockSize = 10 * 1024 * 1024l;

        private FileSystem fs = null;
        private LocalFileSystem lfs = null;
        private PerfectFileMetadata metadata;
        private Path currentDataPartPath = null;

        private FSDataOutputStream outPart;
        private FSDataOutputStream outTmpIndex;
        private FSDataOutputStream outNames;
        long remainPartSize = 0;

        public Writer(Configuration conf, Path dirName, int bucketCapacity, int replication) throws IOException {
            this.conf = conf;
            this.dirName = dirName;

            fs = FileSystem.get(conf);
            lfs = LocalFileSystem.getLocal(conf);

            metadata = new PerfectFileMetadata(fs, getMetadataPath());
            metadata.setRepl(replication);

            if (!fs.exists(dirName)) {

                fs.mkdirs(dirName);

                metadata.getDirectory().init(newBucket());
                this.currentDataPartPath = newPartFile();
                metadata.setCurrentDataPartPath(currentDataPartPath.getName());
                metadata.setBucketCapacity(bucketCapacity);
                metadata.writeMetadata();

                newFile(new Path(dirName, NAMES), true);
            } else {
                currentDataPartPath = new Path(dirName, metadata.getCurrentDataPartPath());
            }
            metadata.setBucketCapacity(bucketCapacity);

            outNames = fs.append(new Path(dirName, NAMES));
            outPart = fs.append(currentDataPartPath);
            remainPartSize = partMaxSize - outPart.getPos();
            recoveryOnFailure();
        }

        public Writer(Configuration conf, Path dirName) throws IOException {
            this(conf, dirName, Integer.MAX_VALUE, (short) conf.getInt("dfs.replication", 3));
        }

        public void putFromLocal(String key, Path filePath) throws IOException {
            put(lfs, key, lfs.getFileStatus(filePath));
        }

        public void putFromLocal(Path filePath) throws IOException {
            put(lfs, filePath.getName(), lfs.getFileStatus(filePath));
        }

        public void put(String key, Path filePath) throws IOException {
            put(fs, key, fs.getFileStatus(filePath));
        }

        public void put(Path filePath) throws IOException {
            put(fs, filePath.getName(), fs.getFileStatus(filePath));
        }

        private void put(FileSystem fs, String key, Path filePath) throws IOException {
            put(fs, key, fs.getFileStatus(filePath));
        }

        public void put(String key, FileStatus fileStatus) throws IOException {
            put(fs, key, fileStatus);
        }

        public void put(FileStatus fileStatus) throws IOException {
            put(fs, fileStatus.getPath().getName(), fileStatus);
        }

        public void putFromLocal(String key, FileStatus fileStatus) throws IOException {
            put(lfs, key, fileStatus);
        }

        public void putFromLocal(FileStatus fileStatus) throws IOException {
            put(lfs, fileStatus.getPath().getName(), fileStatus);
        }

        private void put(FileSystem srcfs, String key, FileStatus fileStatus) throws IOException {
            if (fileStatus.isDirectory()) {
                throw new IOException("Cannot append a directory to the perfect file");
            }

            if (fileStatus.getLen() > blockSize) {
                throw new FileSystemException(fileStatus.getPath().getName() + " is not a small. The file size is too big");
            }

            //read file content
            byte[] bs = new byte[(int) fileStatus.getLen()];
            try (FSDataInputStream in = srcfs.open(fileStatus.getPath())) {
                in.readFully(bs);
            }

            BytesWritable bw = new BytesWritable(PerfectFilesUtil.compress(bs));
            IntWritable iw = new IntWritable(bs.length);

            //build the metadata record
            BucketEntry be = new BucketEntry();
            be.setFileNameHash(PerfectFilesUtil.getHash(key));
            be.setPartFilePosition(metadata.getUsedPartFilePosition());
            be.setOffset(outPart.getPos());

            //write data
            iw.write(outPart);
            bw.write(outPart);
            outPart.hflush();

            be.setSize((int) (outPart.getPos() - be.getOffset()));

            //add metadata  to temporary index
            if (outTmpIndex == null) {
                outTmpIndex = fs.append(newFile(getTemporaryIndexFile(), false));
            }
            be.write(outTmpIndex);
            outTmpIndex.hflush();

            //
            addBucketEntry(be);
            new Text(key).write(outNames);

            //
            if (remainPartSize <= 0) {
                remainPartSize -= outPart.getPos();
                currentDataPartPath = newPartFile();
                metadata.setCurrentDataPartPath(currentDataPartPath.getName());
                metadata.writeMetadata();
            }

        }

        @Override
        public void close() throws IOException {

            if (outPart != null) {
                outPart.close();
            }
            if (outTmpIndex != null) {
                outTmpIndex.close();
            }

            if (outNames != null) {
                outNames.close();
            }

            List<Bucket> buckets = flushBucketsData();
            for (Bucket bucket : buckets) {
                metadata.getPerfectTableHolder().reloadBucketDictionary(bucket);
            }
            metadata.writeMetadata();

            //delete temporary index file
            if (fs.exists(getTemporaryIndexFile())) {
                fs.delete(getTemporaryIndexFile(), true);
            }
        }

        /**
         * A bucket of the hash function is represented by two files in HDFS
         * (index-*, perfect-*). This function save by appending the information
         * of a file in the corresponding bucket index file and save the file
         * name hash in the perfect file.
         *
         * entry1 entry contain the file information
         *
         * @throws IOException
         */
        private synchronized void addBucketEntry(BucketEntry entry) throws IOException {
            Bucket bucket = metadata.getDirectory().getBucketByEntryKey(entry.getFileNameHash());

            //add the index record to bucket
            bucket.getNewEntry1s().add(entry);
            bucket.setSize(bucket.getSize() + 1);

            //split the bucket if full
            if (bucket.getSize() > metadata.getBucketCapacity()) {
                splitBucket(bucket, entry.getFileNameHash());
            }
        }

        private void splitBucket(Bucket toSplitBucket, long key) throws IOException {
            Bucket newBucket = newBucket();

            if (toSplitBucket.getLocalDepth() == metadata.getDirectory().getGlobalDepth()) {
                metadata.getDirectory().doubleSize();
            }
            toSplitBucket.setLocalDepth(metadata.getDirectory().getGlobalDepth());
            newBucket.setLocalDepth(metadata.getDirectory().getGlobalDepth());

            //
            int[] poss = PerfectFilesUtil.checkSplitPositionsInDirectory(key, metadata.getDirectory().getGlobalDepth());
            int pos1 = poss[0];
            int pos2 = poss[1];
            metadata.getDirectory().putBucket(toSplitBucket, pos1);
            metadata.getDirectory().putBucket(newBucket, pos2);

            int p;

            for (BucketEntry be : toSplitBucket.getNewEntry1s()) {
                p = (int) metadata.getDirectory().positionInDirectory(be.getFileNameHash());
                if (p == pos2) {
                    newBucket.addnewEntry(be);
                    toSplitBucket.deleteEntry(be);
                }

            }

            try (FSDataInputStream in1 = fs.open(toSplitBucket.getPath())) {

                //redistribute data into the new bucket
                while (in1.available() > 0) {
                    BucketEntry entry1 = new BucketEntry();
                    entry1.readFields(in1);

                    p = (int) metadata.getDirectory().positionInDirectory(entry1.getFileNameHash());
                    if (p == pos2) {
                        newBucket.addnewEntry(entry1);
                        toSplitBucket.deleteEntry(entry1);
                    }
                }
            }
            toSplitBucket.getNewEntry1s().removeAll(toSplitBucket.getDeletedEntry1s());
        }

        private Bucket newBucket() throws IOException {
            int position = metadata.getIndexLastPosition() + 1;
            Bucket bucket = new Bucket();
            bucket.setLocalDepth(metadata.getDirectory().getGlobalDepth());
            bucket.setPath(new Path(dirName, PerfectFile.INDEX_NAME + position));

            newFile(bucket.getPath(), false);

            metadata.setIndexLastPosition(position);
            return bucket;
        }

        /**
         * Create a new data part-* file and return the path
         *
         * @return The path to the part-* file
         * @throws IOException
         */
        private Path newPartFile() throws IOException {
            int position = metadata.getUsedPartFilePosition() + 1;
            Path p = newPartFile(position, false);
            metadata.setUsedPartFilePosition(position);
            return p;
        }

        private Path newPartFile(int position, boolean overwrite) throws IOException {
            Path p = getPartFilePath(position);
            return newFile(p, overwrite);
        }

        private FSDataOutputStream getInputInLazyPersist(Path path) throws IOException {
            return getInputInLazyPersist(path, false, false);
        }

        private FSDataOutputStream getInputInLazyPersist(Path path, boolean lazyPersist, boolean overwrite) throws IOException {
            return getInputInLazyPersist(path, lazyPersist, overwrite,
                    conf.getInt(IO_FILE_BUFFER_SIZE_KEY, IO_FILE_BUFFER_SIZE_DEFAULT),
                    blockSize,
                    metadata.getRepl());
        }

        private FSDataOutputStream getInputInLazyPersist(Path path, boolean lazyPersist, boolean overwrite, int bufferLength, long blockSize, int replicationFactor) throws IOException {

            if (fs.exists(path)) {
                if (overwrite) {
                    fs.delete(path, true);
                } else {
                    return fs.append(path, bufferLength);
                }
            }
//            if (lazyPersist) {
//                return fs.create(
//                        path,
//                        FsPermission.getFileDefault(),
//                        EnumSet.of(CreateFlag.CREATE, CreateFlag.LAZY_PERSIST),
//                        conf.getInt(IO_FILE_BUFFER_SIZE_KEY, IO_FILE_BUFFER_SIZE_DEFAULT),
//                        (short) metadata.getRepl(),
//                        indexBlockSize,
//                        null);
//            } else {
            return fs.create(path, overwrite, conf.getInt(IO_FILE_BUFFER_SIZE_KEY, IO_FILE_BUFFER_SIZE_DEFAULT), (short) metadata.getRepl(), blockSize);
//            }

        }

        private Path getTemporaryIndexFile() throws IOException {
            return new Path(dirName, PerfectFile.TEMPORARY_INDEX_NAME);
        }

        private Path newFile(Path p, boolean overwrite) throws IOException {
            getInputInLazyPersist(p, false, overwrite).close();
            return p;
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

        private void recoveryOnFailure() throws IOException {
            Path tmpIndex = getTemporaryIndexFile();

            if (fs.exists(tmpIndex)) {
                FSDataInputStream is = fs.open(tmpIndex);

                while (is.available() > 0) {
                    BucketEntry entry = new BucketEntry();
                    entry.readFields(is);
                    addBucketEntry(entry);
                }
                flushBucketsData();
                fs.delete(tmpIndex, true);
            }
        }

        private synchronized List<Bucket> flushBucketsData() {
            List<Bucket> buckets = new ArrayList<>();
            metadata.getDirectory().getBuckets().stream().forEach(b -> {
                try {

                    if (b.needUpdate()) {
                        List<BucketEntry> bes = getBucketAllEntries(b);

                        bes.removeAll(b.getDeletedEntry1s());

                        bes.addAll(b.getNewEntry1s());

                        bes.sort((x, y) -> PerfectTableHolder.compare(x.getFileNameHash(), y.getFileNameHash()));

                        try (FSDataOutputStream out = fs.append(newFile(b.getPath(), true))) {
                            for (BucketEntry be : bes) {
                                be.write(out);
                            }
                        }
                        b.clear();
                        buckets.add(b);
                    }
                } catch (IOException ex) {
                    Logger.getLogger(PerfectFile.class.getName()).log(Level.SEVERE, null, ex);
                }
            });
            return buckets;
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

    }

    public static class Reader implements java.io.Closeable {

        private final Configuration conf;
        private final Path dirName;
        private final FileSystem fs;
        private final PerfectFileMetadata metadata;

        private Path metadataPath = null;

        public Reader(Configuration conf, Path dirName) throws IOException {
            this.conf = conf;
            this.dirName = dirName;

            this.fs = FileSystem.get(conf);

            if (!fs.exists(dirName)) {
                throw new IOException("The file " + dirName + " is not found");
            }

            this.metadata = new PerfectFileMetadata(fs, getMetadataPath());

            //recoveryOnFailure
            if (fs.exists(getTemporaryIndexFile())) {
                new Writer(conf, dirName, -1, 1).recoveryOnFailure();
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

        public List<String> keys() throws IOException {
            FSDataInputStream in = fs.open(new Path(dirName, NAMES));
            List<String> keys = new LinkedList<>();
            Text text = new Text();
            while (in.available() > 0) {
                text.readFields(in);
                keys.add(text.toString());
            }
            return keys;
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

        private Path getMetadataPath() {
            if (metadataPath == null) {
                metadataPath = new Path(dirName, PerfectFile.METADATA_NAME);
            }
            return metadataPath;
        }

        private Path getPartFilePath(int position) {
            return new Path(dirName, PART_NAME + position);
        }

        private Path getTemporaryIndexFile() throws IOException {
            return new Path(dirName, PerfectFile.TEMPORARY_INDEX_NAME);
        }

        @Override
        public void close() throws IOException {

        }

    }
}
