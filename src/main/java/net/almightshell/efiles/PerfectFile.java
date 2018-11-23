/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.almightshell.efiles;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.protobuf.ByteString;
import eu.danieldk.dictomaton.DictionaryBuilder;
import eu.danieldk.dictomaton.DictionaryBuilderException;
import eu.danieldk.dictomaton.PerfectHashDictionary;
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
import java.util.stream.Collectors;
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
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 *
 * @author Shell
 */
public class PerfectFile {

    private static final Log LOG = LogFactory.getLog(PerfectFile.class);
    private static final String INDEX_NAME = "index-";
    private static final String PERFECT_NAME = "perfect-";
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
    private Cache<Long, BucketEntry1> cache = null;
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

    public static PerfectFile open(Configuration conf, Path filePath, int bucketCapacity) throws IOException, Exception {
        return new PerfectFile(conf, filePath, bucketCapacity, false, false, false);
    }

    public static PerfectFile open(Configuration conf, Path filePath, int bucketCapacity, boolean cacheEnabled) throws IOException, Exception {
        return new PerfectFile(conf, filePath, bucketCapacity, false, cacheEnabled, false);
    }

    public static PerfectFile open(Configuration conf, Path filePath, boolean cacheEnabled) throws IOException, Exception {
        return new PerfectFile(conf, filePath, -1, false, cacheEnabled, false);
    }

    public static PerfectFile open(Configuration conf, Path filePath) throws IOException, Exception {
        return new PerfectFile(conf, filePath, -1, false, false, false);
    }

    private synchronized void put(FSDataOutputStream out, String key, Writable writable) throws IOException {
        BucketEntry1 entry1 = new BucketEntry1();
        entry1.setFileNameHash(getHash(key));
        entry1.setPartFilePosition(metadata.getUsedPartFilePosition());
        entry1.setOffset((int) out.getPos());

        BucketEntry2 entry2 = new BucketEntry2();
        entry2.setFileName(key);

        //copy the file content
        writable.write(out);
        entry1.setSize((int) (out.getPos() - entry1.getOffset()));
        addBucketEntry(entry1, entry2);
    }

    private void put(FSDataOutputStream out, FileStatus status) throws IOException {
        if (status.getLen() > blockSize) {
            throw new FileSystemException(status.getPath().getName() + " is not a small. The file size is too big");
        }

        byte[] bs = new byte[(int) status.getLen()];
        fs.open(status.getPath()).readFully(bs);
        put(out, status.getPath().getName(), new BytesWritable(bs));
    }

    public void put(Path path) throws IOException {
        if (fs.getUsed(currentDataPartPath) >= partMaxSize) {
            currentDataPartPath = newPartFile();
            metadata.setCurrentDataPartPath(currentDataPartPath.getName());
        }

        try (FSDataOutputStream out = fs.append(currentDataPartPath)) {
            put(out, fs.getFileStatus(path));
        }
        writeMetadata();
    }

    public void put(String key, Writable writable) throws IOException {
        if (fs.getUsed(currentDataPartPath) >= partMaxSize) {
            currentDataPartPath = newPartFile();
            metadata.setCurrentDataPartPath(currentDataPartPath.getName());
        }

        try (FSDataOutputStream out = fs.append(currentDataPartPath)) {
            put(out, key, writable);
        }
        writeMetadata();
    }

    public void putAll(Path dirpath, boolean recursive) throws IOException {
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
        writeMetadata();
    }

    public List<BucketEntry1> listFiles() throws IOException {

        List<BucketEntry1> files = new ArrayList<>();
        metadata.getDirectory().getBuckets().stream().forEach(bucket -> {
            try (FSDataInputStream in = fs.open(bucket.getPath1())) {

                while (in.available() > 0) {
                    BucketEntry1 entry = new BucketEntry1();
                    entry.readFields(in);
                    files.add(entry);
                }
            } catch (IOException ex) {
                Logger.getLogger(PerfectFile.class.getName()).log(Level.SEVERE, null, ex);
            }
        });

        return files;
    }

    public InputStream get(String key) throws IOException {
        BucketEntry1 entry1 = null;
        long keyHash = getHash(key);

        //get metadata from cache
        if (isCacheEnabled()) {
            entry1 = cache.getIfPresent(keyHash);
        }

        if (entry1 == null) {
            if (isPerfectModeEnabled()) {

            } else {
                entry1 = getBucketEntry(keyHash);
            }
        }

        if (entry1 == null) {
            throw new FileNotFoundException(key + " not found in " + filePath.getName());
        }
        cache.put(keyHash, entry1);

        return get(entry1);
    }

    public InputStream get(BucketEntry1 entry) throws IOException {
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
    private synchronized void addBucketEntry(BucketEntry1 entry1, BucketEntry2 entry2) throws IOException {
        Bucket bucket = metadata.getDirectory().getBucketByEntryKey(entry1.getFileNameHash());

        //write the index record
        try (FSDataOutputStream out = fs.append(bucket.getPath1())) {
            entry1.write(out);
            bucket.setSize(bucket.getSize() + 1);
        }
        //write the hash key in the perfect file
        try (FSDataOutputStream out = fs.append(bucket.getPath2())) {
            entry2.write(out);
        }

        //split the bucket if full
        if (bucket.getSize() >= metadata.getBucketCapacity()) {
            splitBucket(bucket, entry1.getFileNameHash());
        }

        //add metadata to cache
        if (isCacheEnabled()) {
            cache.put(entry1.getFileNameHash(), entry1);
        }
    }

    /**
     *
     * @param keyHash
     * @return
     * @throws IOException
     */
    private BucketEntry1 getBucketEntry(long keyHash) throws IOException {

        Bucket bucket = metadata.getDirectory().getBucketByEntryKey(keyHash);

        //read the index record
        try (FSDataInputStream in = fs.open(bucket.getPath1())) {
            BucketEntry1 entry = new BucketEntry1();
            while (in.available() > 0) {
                entry.readFields(in);
                if (entry.getFileNameHash() == keyHash) {
                    return entry;
                }
            }
        }
        return null;
    }

    private List<BucketEntry1> getBucketAllEntries(Bucket bucket) throws IOException {

        List<BucketEntry1> bucketEntrys = new ArrayList<>();
        //read the index record
        try (FSDataInputStream in = fs.open(bucket.getPath1())) {

            while (in.available() > 0) {
                BucketEntry1 entry = new BucketEntry1();
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

        List<BucketEntry1> remainEntry1s = new ArrayList<>();
        List<BucketEntry2> remainEntry2s = new ArrayList<>();
        int p;
        try (FSDataOutputStream newOut1 = fs.append(newBucket.getPath1()); FSDataOutputStream newOut2 = fs.append(newBucket.getPath2()); FSDataInputStream in1 = fs.open(toSplitBucket.getPath1()); FSDataInputStream in2 = fs.open(toSplitBucket.getPath2())) {

            //redistribute data into the new bucket
            while (in1.available() > 0) {
                BucketEntry1 entry1 = new BucketEntry1();
                BucketEntry2 entry2 = new BucketEntry2();
                entry1.readFields(in1);
                entry2.readFields(in2);

                p = (int) metadata.getDirectory().positionInDirectory(entry1.hashCode());
                if (p == pos2) {
                    entry1.write(newOut1);
                    entry2.write(newOut2);
                    newBucket.setSize(newBucket.getSize() + 1);
                } else {
                    remainEntry1s.add(entry1);
                    remainEntry2s.add(entry2);
                }
            }
        }

        //update original bucket
        try (FSDataOutputStream out1 = fs.create(toSplitBucket.getPath1(), true, conf.getInt(IO_FILE_BUFFER_SIZE_KEY, IO_FILE_BUFFER_SIZE_DEFAULT), metadata.getRepl(), indexBlockSize);
                FSDataOutputStream out2 = fs.create(toSplitBucket.getPath2(), true, conf.getInt(IO_FILE_BUFFER_SIZE_KEY, IO_FILE_BUFFER_SIZE_DEFAULT), metadata.getRepl(), indexBlockSize)) {
            toSplitBucket.setSize(0);
            for (BucketEntry1 e : remainEntry1s) {
                e.write(out1);
                toSplitBucket.setSize(toSplitBucket.getSize() + 1);
            }

            for (BucketEntry2 e : remainEntry2s) {
                e.write(out2);
            }
        }

    }

    private Bucket newBucket() throws IOException {
        int position = metadata.getIndexLastPosition() + 1;
        Bucket bucket = new Bucket();
        bucket.setLocalDepth(metadata.getDirectory().getGlobalDepth());
        bucket.setPath1(new Path(filePath, INDEX_NAME + position));
        bucket.setPath2(new Path(filePath, PERFECT_NAME + position));

        fs.create(bucket.getPath1(), false, conf.getInt(IO_FILE_BUFFER_SIZE_KEY, IO_FILE_BUFFER_SIZE_DEFAULT), metadata.getRepl(), indexBlockSize).close();
        fs.create(bucket.getPath2(), false, conf.getInt(IO_FILE_BUFFER_SIZE_KEY, IO_FILE_BUFFER_SIZE_DEFAULT), metadata.getRepl(), indexBlockSize).close();

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
        Path p = getPartFilePath(position);
        fs.create(p, false, conf.getInt(IO_FILE_BUFFER_SIZE_KEY, IO_FILE_BUFFER_SIZE_DEFAULT), metadata.getRepl(), blockSize).close();
        metadata.setUsedPartFilePosition(position);
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

    private long getHash(String name) {
        return name.hashCode();
    }

    private Path getPartFilePath(int position) {
        return new Path(filePath, PART_NAME + position);
    }

    public FileSystem getFs() {
        return fs;
    }

}
