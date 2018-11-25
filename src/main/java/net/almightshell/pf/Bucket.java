/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.almightshell.pf;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 *
 * @author Shell
 */
public class Bucket implements Writable {

    private Path path;
    private long localDepth = 0;
    private int size = 0;

    private final List<BucketEntry> newEntry1s = new ArrayList<>();
    private final List<BucketEntry> deletedEntry1s = new ArrayList<>();

    public Bucket(Path path1) {
        this.path = path1;
    }

    public Bucket() {
    }

    public void addnewEntry(BucketEntry be) {
        newEntry1s.add(be);
        size++;
    }

    public void deleteEntry(BucketEntry be) {
        deletedEntry1s.add(be);
        size--;
    }

    public boolean needUpdate() {
        return !this.deletedEntry1s.isEmpty() || !this.newEntry1s.isEmpty();
    }

    public void clear() {
        this.newEntry1s.clear();
        this.deletedEntry1s.clear();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, path.toString());
        out.writeLong(localDepth);
        out.writeInt(size);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        path = new Path(Text.readString(in));
        localDepth = in.readLong();
        size = in.readInt();
    }

    public Path getPath() {
        return path;
    }

    public void setPath(Path path) {
        this.path = path;
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

    public List<BucketEntry> getDeletedEntry1s() {
        return deletedEntry1s;
    }

    public List<BucketEntry> getNewEntry1s() {
        return newEntry1s;
    }

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 67 * hash + Objects.hashCode(this.path);
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

        return path.getName().equals(other.path.getName());
    }

    @Override
    public String toString() {
        return "Bucket{" + "path=" + path + ", localDepth=" + localDepth + ", size=" + size + '}';
    }

}
