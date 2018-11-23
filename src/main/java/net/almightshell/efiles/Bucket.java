/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.almightshell.efiles;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 *
 * @author Shell
 */
public class Bucket implements Writable {

    private Path path1;
    private Path path2;
    private long localDepth = 0;
    private int size = 0;

    public Bucket() {
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, path1.toString());
        Text.writeString(out, path2.toString());
        out.writeLong(localDepth);
        out.writeInt(size);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        path1 = new Path(Text.readString(in));
        path2 = new Path(Text.readString(in));
        localDepth = in.readLong();
        size = in.readInt();
    }

    public Path getPath1() {
        return path1;
    }

    public void setPath1(Path path1) {
        this.path1 = path1;
    }

    public long getLocalDepth() {
        return localDepth;
    }

    public void setLocalDepth(long localDepth) {
        this.localDepth = localDepth;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public Path getPath2() {
        return path2;
    }

    public void setPath2(Path path2) {
        this.path2 = path2;
    }

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 67 * hash + Objects.hashCode(this.path1);
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
        final Bucket other = (Bucket) obj;

        return path1.getName().equals(other.path1.getName());
    }

    @Override
    public String toString() {
        return "Bucket{" + "path=" + path1 + ", localDepth=" + localDepth + ", size=" + size + '}';
    }

}
