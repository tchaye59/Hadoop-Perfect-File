/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.almightshell.efiles;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.io.Writable;

/**
 *
 * @author Shell
 */
public class Directory implements Writable {

    private List<Integer> directory = new ArrayList<>();
    private List<Bucket> buckets = new ArrayList<>();
    private int globalDepth = 0;

    public Directory() {
    }

    public void init(Bucket bucket) {
        directory.clear();
        directory.add(0);
        buckets.clear();
        buckets.add(bucket);
    }

    public void doubleSize() {
        int x = directory.size();
        for (int i = 0; i < x; i++) {
            directory.add(directory.get(i));
        }
        globalDepth++;
    }

    public int positionInDirectory(int hashCode) {
        return EFilesUtil.checkPositionInDirectory(hashCode, getGlobalDepth());
    }

    public Bucket getBucket(int pos) {
        return buckets.get(directory.get(pos));
    }

    public Bucket getBucketByEntryKey(int key) {
        return getBucket(positionInDirectory(key));
    }

    public void putBucket(Bucket bucket, int pos) {
        if (!buckets.contains(bucket)) {
            buckets.add(bucket);
        }
        directory.set(pos, buckets.indexOf(bucket));
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(globalDepth);
        out.writeInt(directory.size());
        directory.stream().forEach((v) -> {
            try {
                out.writeInt(v);
            } catch (IOException ex) {
                Logger.getLogger(Directory.class.getName()).log(Level.SEVERE, null, ex);
            }
        });

        out.writeInt(buckets.size());
        buckets.stream().forEach(e -> {
            try {
                e.write(out);
            } catch (IOException ex) {
                Logger.getLogger(Directory.class.getName()).log(Level.SEVERE, null, ex);
            }
        });
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        globalDepth = in.readInt();

        directory.clear();
        int size = in.readInt();
        while (size > 0) {
            directory.add(in.readInt());
            size--;
        }

        buckets.clear();
        size = in.readInt();
        while (size > 0) {
            Bucket bucket = new Bucket();
            bucket.readFields(in);
            buckets.add(bucket);
            size--;
        }

    }

    public List<Integer> getDirectory() {
        return directory;
    }

    public void setDirectory(List<Integer> directory) {
        this.directory = directory;
    }

    public List<Bucket> getBuckets() {
        return buckets;
    }

    public void setBuckets(List<Bucket> buckets) {
        this.buckets = buckets;
    }

    public int getGlobalDepth() {
        return globalDepth;
    }

    public void setGlobalDepth(int globalDepth) {
        this.globalDepth = globalDepth;
    }

}
