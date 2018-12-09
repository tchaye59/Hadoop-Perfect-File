/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.almightshell.utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import static junit.framework.Assert.assertTrue;
import net.almightshell.pf.PerfectFile;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.MapFile.Writer.Option;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.tools.HadoopArchives;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author Shell
 */
public class FileCreat {

    FileSystem fs;
    Configuration conf;

    String localDataPath = "E:\\hadoop-experiment\\data\\";
    String dataPath = "/data";
    String resultPath = "/result";
    int fileNumber = 0;

    public FileCreat(FileSystem fs, Configuration conf, int fileNumber) {
        this.fs = fs;
        this.conf = conf;
        this.fileNumber = fileNumber;
    }

    public void process() throws FileNotFoundException, IOException, Exception {
        File file = new File("E:\\hadoop-experiment\\results\\creat" + fileNumber + ".txt");
        if (file.exists()) {
            file.delete();
        }
        file.createNewFile();

        try (PrintWriter writer = new PrintWriter(file)) {
            writer.println("Data Size : " + fileNumber);

            long time = 0;

            time = processHDFS();
            writer.println("HDFS : " + time + " ms");

            time = processHAR();
            writer.println("HAR : " + time + " ms");

            time = processMap();
            writer.println("MapFile : " + time + " ms");
            
            time = processPF();
            writer.println("PerfectFile : " + time + " ms");
            
            time = processPFFromLocal();
            writer.println("PerfectFile : " + time + " ms");

        }
    }

    public long processHAR() throws IOException, Exception {

        //delete data
        Path harPath = new Path(resultPath, "har" + fileNumber + ".har");
        if (fs.exists(harPath)) {
            fs.delete(harPath, true);
        }

        long currentTimeMillis = System.currentTimeMillis();

        HadoopArchives har = new HadoopArchives(conf);

        String[] args = new String[6];
        args[0] = "-archiveName";
        args[1] = "har" + fileNumber + ".har";
        args[2] = "-p";
        args[3] = dataPath;
        args[4] = fileNumber + "/*";
        args[5] = resultPath;

        int ret = ToolRunner.run(har, args);
        assertTrue("failed test", ret == 0);

        long time = System.currentTimeMillis() - currentTimeMillis;
        System.out.println("processHAR : " + time);
        return time;
    }

    public long processHDFS() throws IOException {
        File inputDir = new File(localDataPath + "/" + fileNumber);
        Path outPutDir = new Path(dataPath + "/" + fileNumber);

        if (fs.exists(outPutDir)) {
            fs.delete(outPutDir, true);
        }

        long currentTimeMillis = System.currentTimeMillis();

        fs.copyFromLocalFile(new Path(localDataPath + "/" + fileNumber), outPutDir);

        long time = System.currentTimeMillis() - currentTimeMillis;
        System.out.println("processHDFS : " + time);
        return time;
    }

    public long processMap() throws IOException {

        Path outputFile = new Path(resultPath + "/mapFile-" + fileNumber);

        if (fs.exists(outputFile)) {
            fs.delete(outputFile, true);
        }

        long currentTimeMillis = System.currentTimeMillis();
        Option wKeyOpt = MapFile.Writer.keyClass(Text.class);
        SequenceFile.Writer.Option wValueOpt = SequenceFile.Writer.valueClass(BytesWritable.class);

        try (MapFile.Writer writer = new MapFile.Writer(conf, outputFile, wKeyOpt, wValueOpt)) {
            for (FileStatus status : fs.listStatus(new Path(dataPath + "/" + fileNumber))) {
                byte[] bs = new byte[(int) status.getLen()];
                fs.open(status.getPath()).readFully(bs);
                writer.append(new Text(status.getPath().getName()), new BytesWritable(bs));
            }
        }

        long time = System.currentTimeMillis() - currentTimeMillis;
        System.out.println("processMap: " + time);
        return time;

    }

    public long processPF() throws IOException, Exception {

        Path path = new Path(resultPath + "/Ehfile-" + fileNumber);

        if (fs.exists(path)) {
            fs.delete(path, true);
        }

        PerfectFile ef = PerfectFile.newFile(conf, path, 100, false, true);

        long currentTimeMillis = System.currentTimeMillis();
        ef.putAll(new Path(dataPath + "/" + fileNumber));
        long time = System.currentTimeMillis() - currentTimeMillis;
        System.out.println("processPF : " + time);
        return time;
    }

    public long processPFFromLocal() throws IOException, Exception {

        Path path = new Path(resultPath + "/Ehfile-" + fileNumber);

        if (fs.exists(path)) {
            fs.delete(path, true);
        }

        PerfectFile ef = PerfectFile.newFile(conf, path, 100, false, true);

        long currentTimeMillis = System.currentTimeMillis();
        ef.putAllFromLocal(new Path("file:///E://hadoop-experiment//data//135/"), true);
        long time = System.currentTimeMillis() - currentTimeMillis;
        System.out.println("processPFFromLocal : " + time);
        return time;
    }

}
