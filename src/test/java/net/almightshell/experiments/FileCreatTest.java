/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.almightshell.experiments;

import java.net.URL;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author Shell
 */
public class FileCreatTest {

    static String hdfsUrl = "hdfs://192.168.136.129:9000";

    static {
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
    }

    public FileCreatTest() {
    }

    /**
     * Test of process method, of class FileCreat.
     */
    @Test
    public void testProcess() throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", hdfsUrl);
        conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", LocalFileSystem.class.getName());
        conf.setBoolean("dfs.support.append", true);
        FileSystem fs = FileSystem.get(conf);

        FileCreat fc = new FileCreat(fs, conf, 135);
        fc.process();
    }
    
//    @Test
//    public void testCreat() throws Exception {
//        Configuration conf = new Configuration();
//        conf.set("fs.defaultFS", hdfsUrl);
//        conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
//        conf.set("fs.file.impl", LocalFileSystem.class.getName());
//        conf.setBoolean("dfs.support.append", true);
//        FileSystem fs = FileSystem.get(conf);
//
//        FileCreat fc = new FileCreat(fs, conf, 135);
//        fc.processEH();
//    }

}
