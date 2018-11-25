/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.almightshell.experiments;

import eu.danieldk.dictomaton.DictionaryBuilder;
import eu.danieldk.dictomaton.PerfectHashDictionary;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import net.almightshell.pf.BucketEntry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.junit.Test;

/**
 *
 * @author Shell
 */
public class FileAccessingTest {

    static String hdfsUrl = "hdfs://192.168.136.129:9000";

    static {
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
    }

    /**
     * Test of process method, of class FileAccessing.
     */
//    @Test
    public void testProcess() throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", hdfsUrl);
        conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", LocalFileSystem.class.getName());
        conf.setBoolean("dfs.support.append", true);
        FileSystem fs = FileSystem.get(conf);

        FileAccessing fa = new FileAccessing(fs, conf, 135);
        fa.process();
    }
    
    @Test
    public void testProcess2() throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", hdfsUrl);
        conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", LocalFileSystem.class.getName());
        conf.setBoolean("dfs.support.append", true);
        FileSystem fs = FileSystem.get(conf);

        FileAccessing fa = new FileAccessing(fs, conf, 135);
        fa.processEH();
    }
    
//    @Test
    public void testSize() throws Exception {
        
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", hdfsUrl);
        conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", LocalFileSystem.class.getName());
        conf.setBoolean("dfs.support.append", true);
        FileSystem fs = FileSystem.get(conf);
        
        List<String> ses = getAllFileNamesFromBucket(fs);
        PerfectHashDictionary dictionary =  new DictionaryBuilder().addAll(ses).buildPerfectHash();
        
        for (String se : ses) {
            System.out.println(dictionary.number(se)+" : "+se);
        }
         
    }
    
    private List<String> getAllFileNamesFromBucket(FileSystem fs ) throws IOException {
        List<String> strings = new ArrayList<>();
        //read the perfect file  records
        try (FSDataInputStream in = fs.open(new Path("/result/Ehfile-135/index-0"))) {
            while (in.available() > 0) {
                BucketEntry be = new BucketEntry();
                be.readFields(in);
                strings.add(be.getFileNameHash()+"");
            }
        }
        return strings;
    }
    
     

}
