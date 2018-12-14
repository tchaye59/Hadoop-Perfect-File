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
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.io.Writable;

/**
 * This class represent the directory of th extendable hash function
 *
 * @author Shell
 */
public class Directory implements Writable {

    private List<Integer> pointers = new ArrayList<>();
    private List<Bucket> buckets = new ArrayList<>();
    private long globalDepth = 0;

    public Directory() {
    }

    public void init(Bucket bucket) {
        pointers.clear();
        pointers.add(0);
        buckets.clear();
        buckets.add(bucket);
    }

    public void doubleSize() {
        int x = pointers.size();
        for (int i = 0; i < x; i++) {
            pointers.add(pointers.get(i));
        }
        globalDepth++;
    }

    public long positionInDirectory(long hashCode) {
        return PerfectFilesUtil.checkPositionInDirectory(hashCode, getGlobalDepth());
    }

    public Bucket getBucket(long pos) {
        return buckets.get(pointers.get((int) pos));
    }

    public Bucket getBucketByEntryKey(long key) {
        if (buckets.size() == 1) {
            return buckets.get(0);
        }
        return getBucket(positionInDirectory(key));
    }

    public void putBucket(Bucket bucket, int pos) {
        if (!buckets.contains(bucket)) {
            buckets.add(bucket);
        }
        pointers.set(pos, buckets.indexOf(bucket));
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(globalDepth);
        out.writeInt(pointers.size());
        pointers.stream().forEach((v) -> {
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
        globalDepth = in.readLong();

        pointers.clear();
        int size = in.readInt();
        while (size > 0) {
            pointers.add(in.readInt());
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

    public List<Integer> getPointers() {
        return pointers;
    }

    public void setPointers(List<Integer> pointers) {
        this.pointers = pointers;
    }

    public List<Bucket> getBuckets() {
        return buckets;
    }

    public void setBuckets(List<Bucket> buckets) {
        this.buckets = buckets;
    }

    public long getGlobalDepth() {
        return globalDepth;
    }

    public void setGlobalDepth(long globalDepth) {
        this.globalDepth = globalDepth;
    }

}
