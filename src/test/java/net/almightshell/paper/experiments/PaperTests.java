/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.almightshell.paper.experiments;

import net.almightshell.utils.PaperTestsHolder;
import eu.danieldk.dictomaton.DictionaryBuilder;
import eu.danieldk.dictomaton.PerfectHashDictionary;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import net.almightshell.pf.BucketEntry;
import net.almightshell.utils.PaperTestsHolder.ExperimentResult;
import net.almightshell.utils.PaperTestsHolder.ExperimentResultItem;
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
public class PaperTests {

    static String hdfsUrl = "hdfs://192.168.136.129:9000";

    static {
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
    }

    /**
     * Test of processAccess method, of class PaperTestsHolder.
     */
    @Test
    public void testProcess() throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", hdfsUrl);
        conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", LocalFileSystem.class.getName());
        conf.setBoolean("dfs.support.append", true);
        FileSystem fs = FileSystem.get(conf);

//        int[] datasets = new int[]{10000, 20000, 40000, 60000};
        int[] datasets = new int[]{100, 200, 400, 600};
        ExperimentResult[][] resultsAccess = new ExperimentResult[2][4];
        ExperimentResult[] resultsCreat = new ExperimentResult[4];

        long t = System.currentTimeMillis();
        int i = 0;
        for (int dataset : datasets) {
            long t1 = System.currentTimeMillis();
            System.out.println("dataset : " + dataset);
            PaperTestsHolder holder = new PaperTestsHolder(fs, conf, dataset);

            System.out.println("Creation...");
            resultsCreat[i] = holder.processCreat();

            System.out.println("Random Access...");
            resultsAccess[0][i] = holder.processAccess(1000, true);
            System.out.println("Random Access with cache effects...");
            resultsAccess[1][i] = holder.processAccess(-1, true);

            System.out.println("Clean experiment data");
            holder.clean();
            System.out.println(dataset + " dataset experiment duration : " + (System.currentTimeMillis() - t1) + "ms");
            i++;
        }

        System.out.println("Generating reports ...");
//metadata size
        File file = new File("E:\\hadoop-experiment\\results\\metadata.txt");
        if (!file.exists()) {
            file.createNewFile();
        }
        try (PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(file, true)))) {
            for (int j = 0; j < datasets.length; j++) {
                writer.println("DataSet: " + datasets[j]);
                for (ExperimentResultItem item : resultsCreat[j].resultItems) {
                    writer.println(item.methodname + " : " + (item.nameNodeMetadataUsage) + " Bytes");
                }
                writer.println("----------------------------------");
                writer.println("----------------------------------");
            }

        }

        file = new File("E:\\hadoop-experiment\\results\\creation.txt");
        if (!file.exists()) {
            file.createNewFile();
        }
        try (PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(file, true)))) {
            for (int j = 0; j < datasets.length; j++) {
                writer.println("DataSet: " + datasets[j]);
                for (ExperimentResultItem item : resultsCreat[j].resultItems) {
                    writer.println(item.methodname + " : " + item.duration + " ms");
                }
                writer.println("----------------------------------");
                writer.println("----------------------------------");
            }
        }

        file = new File("E:\\hadoop-experiment\\results\\access1.txt");
        if (!file.exists()) {
            file.createNewFile();
        }
        try (PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(file, true)))) {
            for (int j = 0; j < datasets.length; j++) {
                writer.println("DataSet: " + datasets[j]);
                for (ExperimentResultItem item : resultsAccess[0][j].resultItems) {
                    writer.println(item.methodname + " : " + item.duration + " ms");
                }
                writer.println("----------------------------------");
                writer.println("----------------------------------");
            }
        }

        file = new File("E:\\hadoop-experiment\\results\\access2.txt");
        if (!file.exists()) {
            file.createNewFile();
        }
        try (PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(file, true)))) {
            for (int j = 0; j < datasets.length; j++) {
                writer.println("DataSet: " + datasets[j]);
                for (ExperimentResultItem item : resultsAccess[1][j].resultItems) {
                    writer.println(item.methodname + " : " + item.duration + " ms");
                }
                writer.println("----------------------------------");
                writer.println("----------------------------------");
            }
        }

        System.out.println("--->Total experiment duration : " + (System.currentTimeMillis() - t) + "ms");

    }

//    @Test
    public void testHPF() throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", hdfsUrl);
        conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", LocalFileSystem.class.getName());
        conf.setBoolean("dfs.support.append", true);
        FileSystem fs = FileSystem.get(conf);

        PaperTestsHolder holder = new PaperTestsHolder(fs, conf, 10000);

        System.out.println("HPF...");
//        System.out.println(holder.uploadToHDFS());

        System.out.println(holder.creatHPF());

    }

//    @Test
    public void testPerfect() throws Exception {

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", hdfsUrl);
        conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", LocalFileSystem.class.getName());
        conf.setBoolean("dfs.support.append", true);
        FileSystem fs = FileSystem.get(conf);

        List<String> ses = getAllFileNamesFromBucket(fs);
        PerfectHashDictionary dictionary = new DictionaryBuilder().addAll(ses).buildPerfectHash();

        for (String se : ses) {
            System.out.println(dictionary.number(se) + " : " + se);
        }

    }

    private List<String> getAllFileNamesFromBucket(FileSystem fs) throws IOException {
        List<String> strings = new ArrayList<>();
        //read the perfect file  records
        try (FSDataInputStream in = fs.open(new Path("/result/Ehfile-135/index-0"))) {
            while (in.available() > 0) {
                BucketEntry be = new BucketEntry();
                be.readFields(in);
                strings.add(be.getFileNameHash() + "");
            }
        }
        return strings;
    }

}
