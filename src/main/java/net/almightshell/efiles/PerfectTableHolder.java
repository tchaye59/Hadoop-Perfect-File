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

    public PerfectTableHolder(FileSystem fs) {
        this.fs = fs;
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
        String dicName = bucket.getPerfectPath().getName();
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
        PerfectHashDictionary dic = new DictionaryBuilder().addAll(getAllPerfectEntries(bucket)).buildPerfectHash();
        map.put(bucket.getPerfectPath().getName(), dic);
        //Call the garbage collector
        System.gc();
        return dic;
    }

    /**
     * Read all records in the perfect file
     *
     * @param bucket represent the perfect file bucket
     * @return
     * @throws IOException
     */
    private List<String> getAllPerfectEntries(Bucket bucket) throws IOException {
        List<String> strings = new ArrayList<>();
        //read the perfect file  records
        try (FSDataInputStream in = fs.open(bucket.getPerfectPath())) {
            while (in.available() > 0) {
                strings.add(Text.readString(in));
            }
        }
        return strings;
    }

}
