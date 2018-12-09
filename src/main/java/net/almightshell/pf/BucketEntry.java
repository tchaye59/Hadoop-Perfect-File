/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.almightshell.pf;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

/**
 * This class represents an entry in the index file
 *
 * @author Shell
 */
public class BucketEntry implements Writable {

    public static final int RECORD_SIZE = 2 * Integer.BYTES + 2 * Long.BYTES;

    private long fileNameHash;
    private int partFilePosition;
    private long offset;
    private int size;

    public BucketEntry() {
    }

    public BucketEntry(long fileNameHash, int partFilePosition, int offset, int size) {
        this.fileNameHash = fileNameHash;
        this.partFilePosition = partFilePosition;
        this.offset = offset;
        this.size = size;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(fileNameHash);
        out.writeInt(partFilePosition);
        out.writeLong(offset);
        out.writeInt(size);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        fileNameHash = in.readLong();
        partFilePosition = in.readInt();
        offset = in.readLong();
        size = in.readInt();
    }

    public long getFileNameHash() {
        return fileNameHash;
    }

    public void setFileNameHash(long fileNameHash) {
        this.fileNameHash = fileNameHash;
    }

    public int getPartFilePosition() {
        return partFilePosition;
    }

    public void setPartFilePosition(int partFilePosition) {
        this.partFilePosition = partFilePosition;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    @Override
    public int hashCode() {
        int hash = 5;
        hash = 89 * hash + (int) (this.fileNameHash ^ (this.fileNameHash >>> 32));
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final BucketEntry other = (BucketEntry) obj;
        if (this.fileNameHash != other.fileNameHash) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "BucketEntry{" + "fileNameHash=" + fileNameHash + ", partFilePosition=" + partFilePosition + ", offset=" + offset + ", size=" + size + '}';
    }

}
