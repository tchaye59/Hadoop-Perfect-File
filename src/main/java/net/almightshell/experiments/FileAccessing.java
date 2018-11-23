/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.almightshell.experiments;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;
import static junit.framework.Assert.assertTrue;
import net.almightshell.efiles.PerfectFile;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.HarFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.tools.HadoopArchives;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author Shell
 */
public class FileAccessing {

    FileSystem fs;
    Configuration conf;

    String localDataPath = "E:\\hadoop-experiment\\data\\";
    String dataPath = "/data";
    String resultPath = "/result";
    int fileNumber = 0;

    List<String> fileNameList = new ArrayList<>();
    List<String> fileNameSubList = new ArrayList<>();

    PerfectFile ef = null;

    public FileAccessing(FileSystem fs, Configuration conf, int fileNumber) throws IOException {
        this.fs = fs;
        this.conf = conf;
        this.fileNumber = fileNumber;

        for (FileStatus fs1 : fs.listStatus(new Path(dataPath + "/" + fileNumber))) {
            fileNameList.add(fs1.getPath().getName());
        }
        fileNameSubList = fileNameList;
    }

    public void process() throws FileNotFoundException, IOException, Exception {
        File file = new File("E:\\hadoop-experiment\\results\\access" + fileNameSubList.size() + ".txt");
        if (file.exists()) {
            file.delete();
        }
        file.createNewFile();

        try (PrintWriter writer = new PrintWriter(file)) {
            writer.println("Accessed data Size : " + fileNameSubList.size());

            long time = 0;
            
            time = processHAR();
            writer.println("HAR : " + time + " ms");
            
            time = processHDFS();
            writer.println("HDFS : " + time + " ms");
            
            time = processMap();
            writer.println("MapFile : " + time + " ms");
            
            time = processEH();
            writer.println("EFile : " + time + " ms");
            
            time = processEH();
            writer.println("EFile : " + time + " ms");

        }
    }

    public long processHAR() throws IOException, Exception {
        
        File file = new File("E:\\hadoop-experiment\\results", "har");
        if (file.exists()) {
            file.delete();
        }
        file.mkdirs();
        //
        long currentTimeMillis = System.currentTimeMillis();
        
        
         Path parent = new Path("har:///" + resultPath, "har" + fileNumber + ".har");
         FileSystem hfs = parent.getFileSystem(conf);
        
        for (String name : fileNameSubList) {
            Path harPath = new Path("har:///" + resultPath, "har" + fileNumber + ".har/" +"/"+fileNumber+"/"+ name);
            IOUtils.copyBytes(hfs.open(harPath), new FileOutputStream(new File(file, name)), conf);
        }

        long time = System.currentTimeMillis() - currentTimeMillis;
        System.out.println("processHAR : " + time);
        return time;
    }

    public long processHDFS() throws IOException {
        File file = new File("E:\\hadoop-experiment\\results", "hdfs");
        if (file.exists()) {
            file.delete();
        }
        file.mkdirs();

        long currentTimeMillis = System.currentTimeMillis();

        for (String name : fileNameSubList) {
            fs.copyToLocalFile(new Path(dataPath + "/" + fileNumber + "/" + name), new Path("E:\\hadoop-experiment\\results", "hdfs"));
        }

        long time = System.currentTimeMillis() - currentTimeMillis;
        System.out.println("processHDFS : " + time);
        return time;
    }

    public long processMap() throws IOException {

        File file = new File("E:\\hadoop-experiment\\results", "map");
        if (file.exists()) {
            file.delete();
        }
        file.mkdirs();

        long currentTimeMillis = System.currentTimeMillis();
        MapFile.Reader reader = new MapFile.Reader(new Path(resultPath + "/mapFile-" + fileNumber), conf);

        for (String name : fileNameSubList) {
            BytesWritable bw = new BytesWritable();
            reader.get(new Text(name), bw);

            File f = new File(file, name);
            f.createNewFile();
            IOUtils.copyBytes(new ByteArrayInputStream(bw.getBytes()), new FileOutputStream(f), conf);
        }

        long time = System.currentTimeMillis() - currentTimeMillis;
        System.out.println("processMap: " + time);
        return time;

    }

    public long processEH() throws IOException, Exception {
        File file = new File("E:\\hadoop-experiment\\results", "efile");
        if (file.exists()) {
            file.delete();
        }
        file.mkdirs();

        if (ef == null) {
            ef = PerfectFile.open(conf, new Path(resultPath + "/Ehfile-" + fileNumber),true);
        }

        long currentTimeMillis = System.currentTimeMillis();
        for (String name : fileNameSubList) {
            File f = new File(file, name);
            f.createNewFile();
            IOUtils.copyBytes(ef.get(name), new FileOutputStream(f), conf);
        }
        long time = System.currentTimeMillis() - currentTimeMillis;
        System.out.println("processEH : " + time);
        return time;
    }

}
