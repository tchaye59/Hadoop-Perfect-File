/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.almightshell.efiles;

import eu.danieldk.dictomaton.DictionaryBuilder;
import eu.danieldk.dictomaton.DictionaryBuilderException;
import eu.danieldk.dictomaton.PerfectHashDictionary;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;

/**
 *
 * @author Shell
 */
public class PerfectTableHolder {

    HashMap<String, PerfectHashDictionary> map = new HashMap<>();
    FileSystem fs = null;
    private PerfectFile pFile = null;

    PerfectTableHolder(PerfectFile pFile) {
        this.pFile = pFile;
    }

    /**
     * This function use the minimal perfect hash dictionary to get the position
     * of a file index record in the index file
     *
     * @param bucket
     * @param entryHash
     * @return
     * @throws IOException
     * @throws DictionaryBuilderException
     */
    public int get(Bucket bucket, int entryHash) throws IOException, DictionaryBuilderException {
        String dicName = bucket.getPath2().getName();
        if (!map.containsKey(dicName)) {
            return reloadBucketDictionary(bucket).number(entryHash + "");
        }
        int x = map.get(dicName).number(entryHash + "");
        if (x < 0) {
            return reloadBucketDictionary(bucket).number(entryHash + "");
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
    private PerfectHashDictionary reloadBucketDictionary(Bucket bucket) throws IOException, DictionaryBuilderException {
        pFile.readMetadata();
        PerfectHashDictionary dic = new DictionaryBuilder().addAll(getAllFileNamesFromBucket(bucket)).buildPerfectHash();
        map.put(bucket.getPath2().getName(), dic);
        pFile.writeMetadata();
        return dic;
    }

    /**
     * Read all records in the perfect file
     *
     * @param bucket represent the perfect file bucket
     * @return
     * @throws IOException
     */
    private List<String> getAllFileNamesFromBucket(Bucket bucket) throws IOException {
        List<String> strings = new ArrayList<>();
        //read the perfect file  records
        try (FSDataInputStream in = fs.open(bucket.getPath2())) {
            while (in.available() > 0) {
                BucketEntry2 entry2 = new BucketEntry2();
                entry2.readFields(in);
                strings.add(entry2.getFileName());
            }
        }
        return strings;
    }

    public HashMap<String, PerfectHashDictionary> getMap() {
        return map;
    }

    public void setMap(HashMap<String, PerfectHashDictionary> map) {
        this.map = map;
    }

}
