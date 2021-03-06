package net.almightshell.pf;

import it.unimi.dsi.bits.BitVector;
import it.unimi.dsi.bits.LongArrayBitVector;
import it.unimi.dsi.bits.TransformationStrategies;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.sux4j.mph.HollowTrieMonotoneMinimalPerfectHashFunction;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 *
 * @author Shell
 */
public class PerfectTableHolder implements Writable {

    private final HashMap<String, HollowTrieMonotoneMinimalPerfectHashFunction> map = new HashMap<>();
    private FileSystem fs = null;

    PerfectTableHolder(FileSystem fs) {
        this.fs = fs;
    }

    /**
     * This function use the minimal perfect hash dictionary to getInputStream
     * the position of a file index record in the index file
     *
     * @param bucket
     * @param entryHash
     * @return
     * @throws IOException
     */
    public long get(Bucket bucket, long entryHash) throws IOException {
        String dicName = bucket.getPath().getName();
        LongArrayBitVector bv = toBitVector(entryHash);
        
        HollowTrieMonotoneMinimalPerfectHashFunction htmmphf = map.get(dicName);
        if (htmmphf==null) {
            return reloadBucketDictionary(bucket).getLong(bv);
        }
        long x = map.get(dicName).getLong(bv);
        if (x < 0) {
            return reloadBucketDictionary(bucket).getLong(bv);
        }
        return x;
    }

    /**
     * Used to build PerfectHashDictionary for a specific bucket
     *
     * @param bucket
     * @return
     * @throws IOException
     * @throws DictionaryBuilderException
     */
    public HollowTrieMonotoneMinimalPerfectHashFunction reloadBucketDictionary(Bucket bucket) throws IOException {
        final HollowTrieMonotoneMinimalPerfectHashFunction<LongArrayBitVector> htmmphf = new HollowTrieMonotoneMinimalPerfectHashFunction(listOf(getAllFileNamesFromBucket(bucket)).iterator(), TransformationStrategies.identity());
        map.put(bucket.getPath().getName(), htmmphf);
        return htmmphf;
    }

    /**
     * Read all records in the perfect file
     *
     * @param bucket represent the perfect file bucket
     * @return
     * @throws IOException
     */
    private Long[] getAllFileNamesFromBucket(Bucket bucket) throws IOException {
        List<Long> keysList = new ArrayList<>();
        //read the perfect file  records
        try (FSDataInputStream in = fs.open(bucket.getPath())) {
            while (in.available() > 0) {
                BucketEntry be = new BucketEntry();
                be.readFields(in);
                keysList.add(be.getFileNameHash());
            }
        }
        Long[] keys = new Long[keysList.size()];
        for (int i = 0; i < keysList.size(); i++) {
            keys[i] = keysList.get(i);

        }
        return keys;
    }

    public static LongArrayBitVector toBitVector(Long key) {
        return LongArrayBitVector.wrap(new long[]{key});
    }

    public static ObjectArrayList<BitVector> listOf(final Long... keys) {
        final ObjectArrayList<BitVector> vectors = new ObjectArrayList<>();

        for (long key : keys) {
            vectors.add(toBitVector(key));
        }

        return vectors;
    }

    public static int compare(long x, long y) {
        return toBitVector(x).compareTo(toBitVector(y));
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(map.size());
        for (String key : map.keySet()) {
            Text.writeString(out, key);
            BytesWritable  bw = new BytesWritable(PerfectFilesUtil.toObjectStream(new PerfectHashDictionaryBean(map.get(key))));
            bw.write(out);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int size = in.readInt();
        map.clear();
        while (size > 0) {
            BytesWritable bw = new BytesWritable();
            
            String key = Text.readString(in);
            bw.readFields(in);
            map.put(key,PerfectFilesUtil.toObject(bw.getBytes(), PerfectHashDictionaryBean.class).getFunction() );
            size--;
        }
    }

}
