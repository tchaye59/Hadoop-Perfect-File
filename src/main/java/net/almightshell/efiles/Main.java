/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.almightshell.efiles;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.io.IOUtils;

/**
 *
 * @author Shell
 */
public class Main {

    static String hdfsUrl = "hdfs://192.168.136.129:9000";

    static {
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
    }
//    new URL("hdfs://192.168.136.129:9000/hadoop.cmd").openStream()

    public static void main(String[] args) throws MalformedURLException {
        try {
            Configuration conf = new Configuration();
            conf.set("fs.defaultFS", hdfsUrl);
            conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
            conf.set("fs.file.impl", LocalFileSystem.class.getName());
            conf.setBoolean("dfs.support.append", true);
            FileSystem fs = FileSystem.get(conf);

            EFile file = EFile.newFile(conf, new Path(hdfsUrl + "/file1"));
            file.putAllFilesFromDir(new Path(hdfsUrl + "/data"), true);

            List<BucketEntry> list = file.listFiles();

            list.parallelStream().forEach(e -> {
                try {
                    InputStream in = file.get(e.getFileName());
                    IOUtils.copyBytes(in, new FileOutputStream("C:\\tmp\\res\\" + e.getFileName()), conf);
                } catch (IOException ex) {
                    Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
                }
            });
        } catch (Exception ex) {
            ex.printStackTrace();
            //Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
