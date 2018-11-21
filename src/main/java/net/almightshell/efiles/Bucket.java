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

    private Path path;
    private Path perfectPath;
    private int localDepth = 0;
    private int size = 0;

    public Bucket() {
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, path.toString());
        Text.writeString(out, perfectPath.toString());
        out.writeInt(localDepth);
        out.writeInt(size);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        path = new Path(Text.readString(in));
        perfectPath = new Path(Text.readString(in));
        localDepth = in.readInt();
        size = in.readInt();
    }

    public Path getPath() {
        return path;
    }

    public void setPath(Path path) {
        this.path = path;
    }

    public int getLocalDepth() {
        return localDepth;
    }

    public void setLocalDepth(int localDepth) {
        this.localDepth = localDepth;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public Path getPerfectPath() {
        return perfectPath;
    }

    public void setPerfectPath(Path perfectPath) {
        this.perfectPath = perfectPath;
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
