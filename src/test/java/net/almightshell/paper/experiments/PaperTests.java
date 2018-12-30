/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.almightshell.paper.experiments;

import net.almightshell.utils.PaperTestsHolder;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.net.URL;
import java.util.UUID;
import net.almightshell.utils.PaperTestsHolder.ExperimentResult;
import net.almightshell.utils.PaperTestsHolder.ExperimentResultItem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.fs.HarFileSystem;
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
//    static String hdfsUrl = "hdfs://10.108.21.223:9000";

    static {
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
    }

    /**
     * Test of processAccess method, of class PaperTestsHolder.
     */
//    @Test
    public void testCreat() throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", hdfsUrl);
        conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", LocalFileSystem.class.getName());
        conf.setBoolean("dfs.support.append", true);
        conf.setInt(HarFileSystem.METADATA_CACHE_ENTRIES_KEY, 0);

        FileSystem fs = FileSystem.get(conf);

        int[] datasets = new int[]{2000, 4000, 6000, 8000, 10000};

        ExperimentResult[] resultsCreat = new ExperimentResult[4];

        long t = System.currentTimeMillis();
        int i = 0;
        for (int dataset : datasets) {
            long t1 = System.currentTimeMillis();
            System.out.println("dataset : " + dataset);
            PaperTestsHolder holder = new PaperTestsHolder(fs, conf, dataset);

            System.out.println("Creation...");
            resultsCreat[i] = holder.processCreat();

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

        System.out.println("--->Total experiment duration : " + (System.currentTimeMillis() - t) + "ms");
    }

    /**
     * Test of processAccess method, of class PaperTestsHolder.
     */
//    @Test
    public void testAccess() throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", hdfsUrl);
        conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", LocalFileSystem.class.getName());
        conf.setBoolean("dfs.support.append", true);
        conf.setInt(HarFileSystem.METADATA_CACHE_ENTRIES_KEY, 0);

        FileSystem fs = FileSystem.get(conf);

        int[] datasets = new int[]{2000, 4000, 6000, 8000, 10000};
        ExperimentResult[][] resultsAccess = new ExperimentResult[2][4];

        long t = System.currentTimeMillis();
        int i = 0;
        for (int dataset : datasets) {
            long t1 = System.currentTimeMillis();
            System.out.println("dataset : " + dataset);
            PaperTestsHolder holder = new PaperTestsHolder(fs, conf, dataset);

            System.out.println("Random Access...");
            resultsAccess[0][i] = holder.processAccess(1000, true);
            System.out.println("Random Access with cache effects...");
            resultsAccess[1][i] = holder.processAccess(-1, true);

            System.out.println("Clean experiment data");
            System.out.println(dataset + " dataset experiment duration : " + (System.currentTimeMillis() - t1) + "ms");
            i++;
        }

        System.out.println("Generating reports ...");

        File file = new File("E:\\hadoop-experiment\\results\\access1.txt");
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

    @Test
    public void testUploadFiles() throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", hdfsUrl);
        conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", LocalFileSystem.class.getName());
        conf.setBoolean("dfs.support.append", true);
        conf.setInt(HarFileSystem.METADATA_CACHE_ENTRIES_KEY, 0);

        FileSystem fs = FileSystem.get(conf);

        int[] datasets = new int[]{600};

        int i = 0;
        for (int dataset : datasets) {

            File file = new File("E:\\hadoop-experiment\\results\\upload.txt");

            try (PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(file, true)))) {

                long t1 = System.currentTimeMillis();
                System.out.println("dataset : " + dataset);
                PaperTestsHolder holder = new PaperTestsHolder(fs, conf, dataset);

                System.out.println("Upload...");
                ExperimentResultItem resultItem = holder.processUploadDataSets();

                System.out.println(dataset + " dataset experiment duration : " + (System.currentTimeMillis() - t1) + "ms");
                i++;

                writer.println("dataset : " + dataset + "   time : " + resultItem.duration + "  MetadataUsage : " + resultItem.nameNodeMetadataUsage);
            }
        }

    }

//    @Test
    public void genearteDataSets() throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", hdfsUrl);
        conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", LocalFileSystem.class.getName());
        conf.setBoolean("dfs.support.append", true);
        conf.setInt(HarFileSystem.METADATA_CACHE_ENTRIES_KEY, 0);

        FileSystem fs = FileSystem.get(conf);

        PaperTestsHolder holder = new PaperTestsHolder(fs, conf, 10000);
        holder.generateDatasetsFiles(new int[]{2000, 4000, 6000, 8000});

    }

//    @Test
    public void testHPF() throws Exception {
//                Configuration conf = new Configuration();
//                conf.set("fs.defaultFS", hdfsUrl);
//                conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
//                conf.set("fs.file.impl", LocalFileSystem.class.getName());
//                conf.setBoolean("dfs.support.append", true);
//                conf.setInt(HarFileSystem.METADATA_CACHE_ENTRIES_KEY, 0);
//                FileSystem fs = FileSystem.get(conf);
//        
//                PaperTestsHolder holder = new PaperTestsHolder(fs, conf, 10000);
//                System.out.println("2000 ->"+holder.calculatePathSize(new Path("/data/2000/files")));

        System.out.println("2000 ->" + bytesToGB(960086016));
        System.out.println("4000 ->" + bytesToGB(1988689920));
        System.out.println("6000 ->" + bytesToGB(2918588416l));
        System.out.println("8000 ->" + bytesToGB(3888906240l));
        System.out.println("10000 ->" + bytesToGB(4884946944l));

    }

    private double bytesToMB(double x) {
        return x / (1024 * 1024);
    }

    private double bytesToGB(double x) {
        return x / (1024 * 1024 * 1024);
    }

}
