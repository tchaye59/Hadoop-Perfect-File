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
public class BucketEntry implements Serializable, Writable {

    private String fileName;
    private String parteFileName;
    private int offset;
    private int size;

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, fileName);
        Text.writeString(out, parteFileName);
        out.writeInt(offset);
        out.writeInt(size);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        fileName = Text.readString(in);
        parteFileName = Text.readString(in);
        offset = in.readInt();
        size = in.readInt();
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public String getParteFileName() {
        return parteFileName;
    }

    public void setParteFileName(String parteFileName) {
        this.parteFileName = parteFileName;
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
        return fileName.hashCode();
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
        if (!Objects.equals(this.fileName, other.fileName)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "BucketEntry{" + "fileName=" + fileName + ", parteFileName=" + parteFileName + ", offset=" + offset + ", size=" + size + '}';
    }

}
