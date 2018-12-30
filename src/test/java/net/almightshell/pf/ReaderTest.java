/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.almightshell.pf;

import java.net.URL;
import static net.almightshell.pf.WriterTest.hdfsUrl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.fs.HarFileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.util.Shell;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author Shell
 */
public class ReaderTest {

    static String hdfsUrl = "hdfs://192.168.136.129:9000";

    static {
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
    }

    @Test
    public void testGet() throws Exception {

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", hdfsUrl);
        conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", LocalFileSystem.class.getName());
        conf.setBoolean("dfs.support.append", true);
        conf.setInt(HarFileSystem.METADATA_CACHE_ENTRIES_KEY, 0);

        FileSystem fs = FileSystem.get(conf);
        FileSystem lfs = LocalFileSystem.getLocal(conf);

        try (PerfectFile.Reader reader = new PerfectFile.Reader(conf, new Path("/hpf.hpf"))) {
            for (FileStatus status : lfs.listStatus(new Path("file:///E:/hadoop-experiment/data/data-600/"))) {
                System.out.println("File : " + status.getPath().getName());
                System.out.println(reader.getBytes(status.getPath().getName()).length);
            }
        }
    }

}
