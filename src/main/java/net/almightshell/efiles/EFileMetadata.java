/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.almightshell.efiles;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

/**
 *
 * @author Shell
 */
public class EFileMetadata implements Writable {

    private int bucketCapacity = 50;
    private int indexLabel = 0;
    private int partLabel = 0;
    private String currentDataPart = null;
    /**
     * the desired replication degree; default is 3 *
     */
    private short repl = 1;

    private Directory directory = new Directory();

    public EFileMetadata() {
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(bucketCapacity);
        out.writeInt(indexLabel);
        out.writeInt(partLabel);
        out.writeUTF(currentDataPart);
        out.writeShort(repl);
        directory.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        bucketCapacity = in.readInt();
        indexLabel = in.readInt();
        partLabel = in.readInt();
        currentDataPart = in.readUTF();
        repl = in.readShort();
        directory.readFields(in);
    }

    public int getBucketCapacity() {
        return bucketCapacity;
    }

    public void setBucketCapacity(int bucketCapacity) {
        this.bucketCapacity = bucketCapacity;
    }

    public short getRepl() {
        return repl;
    }

    public void setRepl(short repl) {
        this.repl = repl;
    }

    public int getIndexLabel() {
        return indexLabel;
    }

    public void setIndexLabel(int indexLabel) {
        this.indexLabel = indexLabel;
    }

    public int getPartLabel() {
        return partLabel;
    }

    public void setPartLabel(int partLabel) {
        this.partLabel = partLabel;
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
