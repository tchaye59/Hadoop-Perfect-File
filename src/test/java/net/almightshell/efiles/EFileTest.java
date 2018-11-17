/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.almightshell.efiles;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Paths;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import static org.hamcrest.CoreMatchers.is;
import org.junit.After;
import org.junit.AfterClass;
import static org.junit.Assert.assertThat;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 * @author Shell
 */
public class EFileTest {

    private static MiniDFSCluster cluster; // use an in-process HDFS cluster for testing
    private static FileSystem fs;

    @BeforeClass
    public static void setUp() throws IOException {
        try {
            Configuration conf = new Configuration();
            if (System.getProperty("test.build.data") == null) {
                System.setProperty("test.build.data", "/tmp");
                System.setProperty("hadoop.home.dir", "D:\\Program Files\\hadoop-2.8.3");
            }
            cluster = new MiniDFSCluster.Builder(conf).build();
            fs = cluster.getFileSystem();
            OutputStream out = fs.create(new Path("/dir/file"));
            out.write("content".getBytes("UTF-8"));
            out.close();
        } catch (Exception exception) {
            exception.printStackTrace();
        }
    }

    @AfterClass
    public static void tearDown() throws IOException {
        if (fs != null) {
            fs.close();
        }
        if (cluster != null) {
            cluster.shutdown();
        }
    }

    @Test(expected = FileNotFoundException.class)
    public void throwsFileNotFoundForNonExistentFile() throws IOException {
        fs.getFileStatus(new Path("no-such-file"));
    }

    @Test
    public void fileStatusForFile() throws IOException {
        Path file = new Path("/dir/file");
        FileStatus stat = fs.getFileStatus(file);
        assertThat(stat.getPath().toUri().getPath(), is("/dir/file"));
        assertThat(stat.isDirectory(), is(false));
        assertThat(stat.getLen(), is(7L));
        assertThat(stat.getReplication(), is((short) 1));
        assertThat(stat.getBlockSize(), is(128 * 1024 * 1024L));
        assertThat(stat.getOwner(), is(System.getProperty("user.name")));
        assertThat(stat.getGroup(), is("supergroup"));
        assertThat(stat.getPermission().toString(), is("rw-r--r--"));
    }

    @Test
    public void fileStatusForDirectory() throws IOException {
        Path dir = new Path("/dir");
        FileStatus stat = fs.getFileStatus(dir);
        assertThat(stat.getPath().toUri().getPath(), is("/dir"));
        assertThat(stat.isDirectory(), is(true));
        assertThat(stat.getLen(), is(0L));

        assertThat(stat.getReplication(), is((short) 0));
        assertThat(stat.getBlockSize(), is(0L));
        assertThat(stat.getOwner(), is(System.getProperty("user.name")));
        assertThat(stat.getGroup(), is("supergroup"));
        assertThat(stat.getPermission().toString(), is("rwxr-xr-x"));
    }

    /**
     * Test of getGlobalDepth method, of class EFile.
     */
    @Test
    public void testGetGlobalDepth() {
    }

    /**
     * Test of setDirectory method, of class EFile.
     */
    @Test
    public void testSetDirectory() {
    }

}
