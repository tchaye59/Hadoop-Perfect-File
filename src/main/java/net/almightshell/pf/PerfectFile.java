/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.almightshell.pf;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import eu.danieldk.dictomaton.DictionaryBuilderException;
import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileSystemException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
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
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;

/**
 *
 * @author Shell
 */
public class PerfectFile {

    private static final Log LOG = LogFactory.getLog(PerfectFile.class);
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

    PerfectFileMetadata metadata = null;
    private Path currentDataPartPath = null;
    private Cache<Long, BucketEntry> cache = null;
    private boolean cacheEnabled = true;
    private boolean perfectModeEnabled = false;

    private PerfectFile(Configuration conf, Path filePath, int bucketCapacity, boolean newFile, boolean cacheEnabled, boolean perfectModeEnabled) throws IOException, Exception {
        this.conf = conf;
        this.filePath = filePath;
        this.cacheEnabled = cacheEnabled;
        this.perfectModeEnabled = perfectModeEnabled;

        fs = FileSystem.get(conf);
        metadata = new PerfectFileMetadata(this, perfectModeEnabled);

        if (isCacheEnabled()) {
            cache = CacheBuilder.newBuilder()
                    .maximumSize(10000)
                    .expireAfterWrite(24, TimeUnit.HOURS)
                    .build();
        }

        if (newFile) {
            if (fs.exists(filePath)) {
                throw new FileAlreadyExistsException("The file " + filePath.getName() + " already exists");
            }
            fs.mkdirs(filePath);

            metadata.getDirectory().init(newBucket());
            this.currentDataPartPath = newPartFile();
            metadata.setCurrentDataPartPath(currentDataPartPath.getName());
            metadata.setBucketCapacity(bucketCapacity);
            writeMetadata();
        } else {
            if (!fs.exists(filePath)) {
                throw new FileNotFoundException("The file " + filePath.getName() + " not found");
            }
            readMetadata();
            currentDataPartPath = new Path(filePath, metadata.getCurrentDataPartPath());
        }
        metadata.setBucketCapacity(bucketCapacity);
    }

    public static PerfectFile newFile(Configuration conf, Path filePath, int bucketCapacity, boolean cacheEnabled, boolean perfectModeEnabled) throws IOException, Exception {
        return new PerfectFile(conf, filePath, bucketCapacity, true, cacheEnabled, perfectModeEnabled);
    }

    public static PerfectFile newFile(Configuration conf, Path filePath, int bucketCapacity) throws IOException, Exception {
        return new PerfectFile(conf, filePath, bucketCapacity, true, false, false);
    }

    public static PerfectFile newFile(Configuration conf, Path filePath, boolean cacheEnabled) throws IOException, Exception {
        return new PerfectFile(conf, filePath, -1, true, cacheEnabled, false);
    }

    public static PerfectFile newFile(Configuration conf, Path filePath) throws IOException, Exception {
        return new PerfectFile(conf, filePath, -1, true, false, false);
    }

    public static PerfectFile open(Configuration conf, Path filePath, int bucketCapacity, boolean cacheEnabled, boolean perfectModeEnabled) throws IOException, Exception {
        return new PerfectFile(conf, filePath, bucketCapacity, false, cacheEnabled, perfectModeEnabled);
    }

    public static PerfectFile open(Configuration conf, Path filePath, boolean cacheEnabled, boolean perfectModeEnabled) throws IOException, Exception {
        return new PerfectFile(conf, filePath, -1, false, cacheEnabled, perfectModeEnabled);
    }

    public static PerfectFile open(Configuration conf, Path filePath, int bucketCapacity) throws IOException, Exception {
        return new PerfectFile(conf, filePath, bucketCapacity, false, false, false);
    }

    public static PerfectFile open(Configuration conf, Path filePath, int bucketCapacity, boolean cacheEnabled) throws IOException, Exception {
        return new PerfectFile(conf, filePath, bucketCapacity, false, cacheEnabled, false);
    }

    public static PerfectFile open(Configuration conf, Path filePath, boolean cacheEnabled) throws IOException, Exception {
        return new PerfectFile(conf, filePath, -1, false, cacheEnabled, true);
    }

    public static PerfectFile open(Configuration conf, Path filePath) throws IOException, Exception {
        return new PerfectFile(conf, filePath, -1, false, false, false);
    }

    private synchronized void put(FSDataOutputStream out, String key, Writable writable) throws IOException {
        BucketEntry entry1 = new BucketEntry();
        entry1.setFileNameHash(PerfectFilesUtil.getHash(key));
        entry1.setPartFilePosition(metadata.getUsedPartFilePosition());
        entry1.setOffset((int) out.getPos());

        //copy the file content
        writable.write(out);
        entry1.setSize((int) (out.getPos() - entry1.getOffset()));
        addBucketEntry(entry1);
    }

    private void put(FSDataOutputStream out, FileStatus status) throws IOException {
        if (status.getLen() > blockSize) {
            throw new FileSystemException(status.getPath().getName() + " is not a small. The file size is too big");
        }

        byte[] bs = new byte[(int) status.getLen()];
        fs.open(status.getPath()).readFully(bs);
        put(out, status.getPath().getName(), new BytesWritable(bs));
    }

    public void put(Path path) throws IOException, DictionaryBuilderException {
        if (fs.getUsed(currentDataPartPath) >= partMaxSize) {
            currentDataPartPath = newPartFile();
            metadata.setCurrentDataPartPath(currentDataPartPath.getName());
        }

        try (FSDataOutputStream out = fs.append(currentDataPartPath)) {
            put(out, fs.getFileStatus(path));
        }

        List<Bucket> buckets = flushBucketsData();
        for (Bucket bucket : buckets) {
            metadata.getPerfectTableHolder().reloadBucketDictionary(bucket);
        }
        writeMetadata();
    }

    public void putAll(Path dirpath, boolean recursive) throws IOException, DictionaryBuilderException {
        FileStatus[] fses = fs.listStatus(dirpath);

        try (FSDataOutputStream out = fs.append(currentDataPartPath)) {
            if (fs.getUsed(currentDataPartPath) >= partMaxSize) {
                currentDataPartPath = newPartFile();
                metadata.setCurrentDataPartPath(currentDataPartPath.getName());
            }

            for (FileStatus fse : fses) {
                put(out, fse);
            }
        }
        List<Bucket> buckets = flushBucketsData();
        for (Bucket bucket : buckets) {
            metadata.getPerfectTableHolder().reloadBucketDictionary(bucket);
        }
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
                Logger.getLogger(PerfectFile.class.getName()).log(Level.SEVERE, null, ex);
            }
        });

        return files;
    }

    public InputStream get(String key) throws IOException, DictionaryBuilderException {
        BucketEntry be = null;
        long keyHash = PerfectFilesUtil.getHash(key);

        //get metadata from cache
        if (isCacheEnabled()) {
            be = cache.getIfPresent(keyHash);
        }

        if (be == null) {
            if (isPerfectModeEnabled()) {
                be = getEntryFromBucketByPerfectTable(keyHash);
            } else {
                be = getEntryFromBucket(keyHash);
            }
        }

        if (be == null) {
            throw new FileNotFoundException(key + " not found in " + filePath.getName());
        }

        if (isCacheEnabled()) {
            cache.put(keyHash, be);
        }

        return get(be);
    }

    public InputStream get(BucketEntry entry) throws IOException {
        byte[] b = new byte[entry.getSize()];
        try (FSDataInputStream in = fs.open(getPartFilePath(entry.getPartFilePosition()))) {
            in.seek(entry.getOffset());
            in.readFully(b);
        }

        return new ByteArrayInputStream(b);
    }

    /**
     * A bucket of the hash function is represented by two files in HDFS
     * (index-*, perfect-*). This function save by appending the information of
     * a file in the corresponding bucket index file and save the file name hash
     * in the perfect file.
     *
     * entry1 entry contain the file information
     *
     * @throws IOException
     */
    private synchronized void addBucketEntry(BucketEntry entry1) throws IOException {
        Bucket bucket = metadata.getDirectory().getBucketByEntryKey(entry1.getFileNameHash());

        //add the index record to bucket
        bucket.getNewEntry1s().add(entry1);
        bucket.setSize(bucket.getSize() + 1);

        //split the bucket if full
        if (bucket.getSize() >= metadata.getBucketCapacity()) {
            splitBucket(bucket, entry1.getFileNameHash());
        }

        //add metadata to cache
        if (isCacheEnabled()) {
            cache.put(entry1.getFileNameHash(), entry1);
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

                    bes.sort((x, y) -> {
                        return String.valueOf(x.getFileNameHash()).compareTo(String.valueOf(y.getFileNameHash()));
                    });

                    try (FSDataOutputStream out = fs.append(newPartFile(b.getPath(), true))) {
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

    /**
     *
     * @param keyHash
     * @return
     * @throws IOException
     */
    private BucketEntry getEntryFromBucket(long keyHash) throws IOException {
        Bucket bucket = metadata.getDirectory().getBucketByEntryKey(keyHash);
        //read the index record
        try (FSDataInputStream in = fs.open(bucket.getPath())) {
            BucketEntry entry = new BucketEntry();
            while (in.available() > 0) {
                entry.readFields(in);
                if (entry.getFileNameHash() == keyHash) {
                    return entry;
                }
            }
        }
        return null;
    }

    private BucketEntry getEntryFromBucketByPerfectTable(long keyHash) throws IOException, DictionaryBuilderException {
        Bucket bucket = metadata.getDirectory().getBucketByEntryKey(keyHash);
        //read the index record

        int pos = metadata.getPerfectTableHolder().get(bucket, keyHash);
        if (pos < 0) {
            return null;
        }
        
        long offSet = (pos-1) * BucketEntry.RECORD_SIZE;

        BucketEntry entry = new BucketEntry();
        try (FSDataInputStream in = fs.open(bucket.getPath())) {
            in.seek(offSet);
            entry.readFields(in);
        }
        return entry;
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
    }

    private Bucket newBucket() throws IOException {
        int position = metadata.getIndexLastPosition() + 1;
        Bucket bucket = new Bucket();
        bucket.setLocalDepth(metadata.getDirectory().getGlobalDepth());
        bucket.setPath(new Path(filePath, INDEX_NAME + position));

        fs.create(bucket.getPath(), false, conf.getInt(IO_FILE_BUFFER_SIZE_KEY, IO_FILE_BUFFER_SIZE_DEFAULT), metadata.getRepl(), indexBlockSize).close();

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
        return newPartFile(p, overwrite);
    }

    private Path newPartFile(Path p, boolean overwrite) throws IOException {
        fs.create(p, overwrite, conf.getInt(IO_FILE_BUFFER_SIZE_KEY, IO_FILE_BUFFER_SIZE_DEFAULT), metadata.getRepl(), blockSize).close();
        return p;
    }

    public void writeMetadata() throws IOException {
        try (FSDataOutputStream out = fs.create(getMetadataPath(), true, conf.getInt(IO_FILE_BUFFER_SIZE_KEY, IO_FILE_BUFFER_SIZE_DEFAULT), metadata.getRepl(), blockSize)) {
            metadata.write(out);
        }
    }

    public void readMetadata() throws IOException {
        if (!fs.exists(getMetadataPath())) {
            writeMetadata();
        } else {
            try (FSDataInputStream in = fs.open(getMetadataPath())) {
                metadata.readFields(in);
            }
        }
    }

    public boolean isCacheEnabled() {
        return cacheEnabled;
    }

    public void setCacheEnabled(boolean cacheEnabled) {
        this.cacheEnabled = cacheEnabled;
    }

    public boolean isPerfectModeEnabled() {
        return perfectModeEnabled;
    }

    public void setPerfectModeEnabled(boolean perfectModeEnabled) {
        this.perfectModeEnabled = perfectModeEnabled;
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

    private Path getPartFilePath(int position) {
        return new Path(filePath, PART_NAME + position);
    }

    public FileSystem getFs() {
        return fs;
    }

}
