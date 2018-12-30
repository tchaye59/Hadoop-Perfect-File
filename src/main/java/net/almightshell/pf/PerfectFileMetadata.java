/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.almightshell.pf;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_KEY;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;

/**
 *
 * @author Shell
 */
public class PerfectFileMetadata implements Writable {

    private int bucketCapacity = Integer.MAX_VALUE;
    private int indexLastPosition = -1;
    private int usedPartFilePosition = -1;
    private String currentDataPart = null;
    /**
     * the desired replication degree; default is 3 *
     */
    private int repl = 1;

    private Directory directory = new Directory();
    private PerfectTableHolder perfectTableHolder = null;
    private final FileSystem fs;
    private final Path metadataPath;

    public PerfectFileMetadata(FileSystem fs, Path metadataPath) throws IOException {
        perfectTableHolder = new PerfectTableHolder(fs);
        this.fs = fs;
        this.metadataPath = metadataPath;
        readMetadata(metadataPath);
    }

    private void readMetadata(Path metadataPath) throws IOException {
        if (!fs.exists(metadataPath)) {
            return;
        }
        try (FSDataInputStream in = fs.open(metadataPath)) {
            this.readFields(in);
        }
    }

    public void writeMetadata() throws IOException {
        try (FSDataOutputStream out = fs.create(metadataPath, true, fs.getConf().getInt(IO_FILE_BUFFER_SIZE_KEY, IO_FILE_BUFFER_SIZE_DEFAULT), (short) getRepl(), PerfectFile.Writer.blockSize)) {
            write(out);
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(bucketCapacity);
        out.writeInt(indexLastPosition);
        out.writeInt(usedPartFilePosition);
        out.writeUTF(currentDataPart);
        perfectTableHolder.write(out);
        directory.write(out);

    }

    @Override
    public void readFields(DataInput in) throws IOException {
        bucketCapacity = in.readInt();
        indexLastPosition = in.readInt();
        usedPartFilePosition = in.readInt();
        currentDataPart = in.readUTF();
        perfectTableHolder.readFields(in);
        directory.readFields(in);

    }

    public int getBucketCapacity() {
        return bucketCapacity;
    }

    public void setBucketCapacity(int bucketCapacity) {
        if (bucketCapacity <= 0) {
            return;
        }
        this.bucketCapacity = bucketCapacity;
    }

    public int getRepl() {
        return repl;
    }

    public void setRepl(int repl) {
        this.repl = repl;
    }

    public int getIndexLastPosition() {
        return indexLastPosition;
    }

    public void setIndexLastPosition(int indexLastPosition) {
        this.indexLastPosition = indexLastPosition;
    }

    public int getUsedPartFilePosition() {
        return usedPartFilePosition;
    }

    public PerfectTableHolder getPerfectTableHolder() {
        return perfectTableHolder;
    }

    public void setUsedPartFilePosition(int usedPartFilePosition) {
        this.usedPartFilePosition = usedPartFilePosition;
    }

    public String getCurrentDataPartPath() {
        return currentDataPart;
    }

    public void setCurrentDataPartPath(String currentDataPart) {
        this.currentDataPart = currentDataPart;
    }

    public Directory getDirectory() {
        return directory;
    }

    public void setDirectory(Directory directory) {
        this.directory = directory;
    }

}
