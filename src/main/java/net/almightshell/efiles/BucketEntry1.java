/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.almightshell.efiles;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.Objects;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 *
 * @author Shell
 */
public class BucketEntry1 implements Writable {
    
    public final int RECORD_SIZE = 1*16 + 3*8;

    private long fileNameHash;
    private int partFilePosition;
    private int offset;
    private int size;

    public BucketEntry1() {
    }

    public BucketEntry1(long fileNameHash, int partFilePosition, int offset, int size) {
        this.fileNameHash = fileNameHash;
        this.partFilePosition = partFilePosition;
        this.offset = offset;
        this.size = size;
    }

    
    
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(fileNameHash);
        out.writeInt(partFilePosition);
        out.writeInt(offset);
        out.writeInt(size);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        fileNameHash = in.readLong();
        partFilePosition = in.readInt();
        offset = in.readInt();
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

    public int getOffset() {
        return offset;
    }

    public void setOffset(int offset) {
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
        final BucketEntry1 other = (BucketEntry1) obj;
        if (this.fileNameHash != other.fileNameHash) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "BucketEntry1{" + "fileNameHash=" + fileNameHash + ", partFilePosition=" + partFilePosition + ", offset=" + offset + ", size=" + size + '}';
    }

}
