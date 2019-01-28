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
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Period;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
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

//    static String hdfsUrl = "hdfs://192.168.136.129:9000";
    static String hdfsUrl = "hdfs://10.108.21.223:9000";

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

        int[] datasets = new int[]{300000, 400000};

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
                    writer.println(item.methodname + " : " + (item.metadataUsage) + " Bytes");
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
    @Test
    public void testAccess() throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", hdfsUrl);
        conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", LocalFileSystem.class.getName());
        conf.setBoolean("dfs.support.append", true);
        conf.setInt(HarFileSystem.METADATA_CACHE_ENTRIES_KEY, 0);

        FileSystem fs = FileSystem.get(conf);

        int[] datasets = new int[]{100000};
        List<ExperimentResult> results = new ArrayList<>();

        long t = System.currentTimeMillis();
        int i = 5;

        boolean enableCache = true;
        int numTry = 5;

        for (int dataset : datasets) {
            System.out.println("dataset : " + dataset);
            PaperTestsHolder holder = new PaperTestsHolder(fs, conf, dataset);
            System.out.println("Random Access...");

            while (numTry > 0) {
                results.add(holder.processAccess(100, enableCache, true));
                numTry--;
            }
            i++;
        }

        System.out.println("Generating reports ...");

        File file = new File("E:\\hadoop-experiment\\results\\access.txt");
        if (!file.exists()) {
            file.createNewFile();
        }
        try (PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(file, true)))) {

            results.stream().map((result) -> {
                writer.println("DataSet: " + result.fileNumber);
                return result;
            }).map((result) -> {
                writer.println("EnableCache: " + enableCache);
                result.resultItems.forEach((item) -> {
                    writer.println(item.methodname + " : " + item.duration + " ms" + " metadataUsage : " + item.metadataUsage);
                });
                return result;
            }).map((_item) -> {
                writer.println("----------------------------------");
                return _item;
            }).forEachOrdered((_item) -> {
                writer.println("----------------------------------");
            });

        }

        System.out.println("--->Total experiment duration : " + (System.currentTimeMillis() - t) + "ms");

    }

//    @Test
    public void testUploadFiles() throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", hdfsUrl);
        conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", LocalFileSystem.class.getName());
        conf.setBoolean("dfs.support.append", true);
        conf.setInt(HarFileSystem.METADATA_CACHE_ENTRIES_KEY, 0);

        FileSystem fs = FileSystem.get(conf);

        int[] datasets = new int[]{300000, 400000};

        try {
            for (int dataset : datasets) {

                File file = new File("E:\\hadoop-experiment\\results\\upload.txt");

                try (PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(file, true)))) {

                    long t1 = System.currentTimeMillis();
                    System.out.println("dataset : " + dataset);
                    PaperTestsHolder holder = new PaperTestsHolder(fs, conf, dataset);

                    System.out.println("Upload...");
                    ExperimentResultItem resultItem = holder.processUploadDataSets();

                    System.out.println(dataset + " dataset experiment duration : " + (System.currentTimeMillis() - t1) + "ms");

                    writer.println("dataset : " + dataset + "   time : " + resultItem.duration + "  MetadataUsage : " + resultItem.metadataUsage);
                }
            }
        } catch (Exception exception) {
            Thread.sleep(10000);
            testUploadFiles();
        }

    }

//    @Test
    public void testDistUploadFiles() throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", hdfsUrl);
        conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", LocalFileSystem.class.getName());
        conf.setBoolean("dfs.support.append", true);

        FileSystem fs = FileSystem.get(conf);

        int[] datasets = new int[]{300000};

        int i = 0;
        for (int dataset : datasets) {

            File file = new File("E:\\hadoop-experiment\\results\\upload.txt");

            try (PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(file, true)))) {

                long t1 = System.currentTimeMillis();
                System.out.println("dataset : " + dataset);
                PaperTestsHolder holder = new PaperTestsHolder(fs, conf, dataset);

                System.out.println("Upload...");
                ExperimentResultItem resultItem = holder.distUploadToHDFS();

                System.out.println(dataset + " dataset experiment duration : " + (System.currentTimeMillis() - t1) + "ms");
                i++;

                writer.println("dataset : " + dataset + "   time : " + resultItem.duration + "  MetadataUsage : " + resultItem.metadataUsage);
            }
        }

    }

//    @Test
    public void testHPF() throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", hdfsUrl);
        conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", LocalFileSystem.class.getName());
        conf.setBoolean("dfs.support.append", true);
        conf.setInt(HarFileSystem.METADATA_CACHE_ENTRIES_KEY, 0);
//        FileSystem fs = FileSystem.get(conf);

//        PaperTestsHolder holder = new PaperTestsHolder(fs, conf, 100000);
        LocalDateTime ld1 = LocalDateTime.of(2019, 1, 11, 11, 39, 04);
        LocalDateTime ld2 = LocalDateTime.of(2019, 1, 11, 16, 55, 57);
        System.out.println(ld1.until(ld2, ChronoUnit.MINUTES));

    }

    private double bytesToMB(double x) {
        return x / (1024 * 1024);
    }

    private double bytesToGB(double x) {
        return x / (1024 * 1024 * 1024);
    }

}
