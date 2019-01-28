/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.almightshell.utils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;
import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;
import net.almightshell.pf.PerfectFile;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.HarFileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.tools.DistCp;
import org.apache.hadoop.tools.DistCpOptions;
import org.apache.hadoop.tools.HadoopArchives;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author Shell
 */
public class PaperTestsHolder {

    public class ExperimentResult {

        public int fileNumber = 0;
        public List<ExperimentResultItem> resultItems = new ArrayList<>();
    }

    public class ExperimentResultItem {

        public String methodname = "";
        //size in byte
        public long metadataUsage = 0;

        public long duration = 0;

        @Override
        public String toString() {
            return "ExperimentResultItem{" + "methodname=" + methodname + ", nameNodeMetadataUsage=" + metadataUsage + ", duration=" + duration + '}';
        }

    }

    FileSystem fs;
    FileSystem lfs;

    Configuration conf;

    String localDataPath = "file:///E:/hadoop-experiment/data/data-";
    String localResultPath = "E:\\hadoop-experiment\\results\\";
    String hdfsDataPath = "/data/";
    String hdfsArchivesDataPath = "";

    String harFilePath = "";
    String mapFilePath = "";
    String hpFilePath = "";
    String hdfsFilesPath = "";

    int fileNumber = 0;

    List<String> fileNameList = new ArrayList<>();
    List<String> fileNameSubList = new ArrayList<>();

    boolean cache = false;

    public PaperTestsHolder(FileSystem fs, Configuration conf, int fileNumber) throws IOException {
        this.fs = fs;
        this.conf = conf;
        this.fileNumber = fileNumber;
        this.localDataPath += fileNumber;
        this.localResultPath += fileNumber;
        this.hdfsDataPath += fileNumber;
        lfs = LocalFileSystem.getLocal(conf);

        this.hdfsArchivesDataPath = hdfsDataPath + "/archives";

        File f = new File(localResultPath);
        if (f.exists()) {
            boolean bool = f.delete();
        }
        f.mkdirs();

        Path path = new Path(hdfsDataPath);
        if (!fs.exists(path)) {
            fs.mkdirs(path);
        }

        harFilePath = hdfsArchivesDataPath + "/har-" + fileNumber + ".har";
        mapFilePath = "/mapfile-" + fileNumber;
        hpFilePath = "/hpf-" + fileNumber + ".hpf";
        hdfsFilesPath = hdfsDataPath + "/files";

        try {
            for (FileStatus listStatu : lfs.listStatus(new Path(localDataPath))) {
                fileNameList.add(listStatu.getPath().getName());
            }
            fileNameSubList = fileNameList;
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public ExperimentResultItem processUploadDataSets() throws FileNotFoundException, IOException, Exception {
        System.out.println("->Upload Files from HDFS");
        return uploadToHDFS();
    }

    public ExperimentResultItem processDistUploadDataSets() throws FileNotFoundException, IOException, Exception {
        System.out.println("->Dist Upload Files from HDFS");
        return distUploadToHDFS();
    }

    public ExperimentResult processCreat() throws FileNotFoundException, IOException, Exception {
        ExperimentResult result = new ExperimentResult();

        File file = new File(localResultPath, "Creat.txt");
        if (!file.exists()) {
            file.createNewFile();
        }

        ExperimentResultItem resultItem = null;
        System.out.println("Starting the archives Creation test...");

        try (PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(file, true)))) {

            System.out.println("->HAR : Creation");
            resultItem = creatHAR();
            result.resultItems.add(resultItem);
            printLog(resultItem, writer, true);
//            System.out.println("->MapFile : Creation");
//            resultItem = creatMapFile();
//            result.resultItems.add(resultItem);
//            printLog(resultItem, writer, true);
//
//            System.out.println("->HPF : Creation");
//            resultItem = creatHPF();
//            result.resultItems.add(resultItem);
//            printLog(resultItem, writer, true);

            writer.println();
            writer.println();
        }
        return result;
    }

    public ExperimentResult processAccess(int fileNumber, boolean cache) throws FileNotFoundException, IOException, Exception {
        return processAccess(fileNumber, cache, true);
    }

    public ExperimentResult processAccess(int fileNumber, boolean cache, boolean random) throws FileNotFoundException, IOException, Exception {
        this.cache = cache;
        if (fileNumber > 0) {
            if (random) {
                Collections.shuffle(fileNameList, new Random(System.nanoTime()));
            }
            if (fileNumber >= fileNameList.size()) {
                fileNameSubList = fileNameList;
            } else {
                fileNameSubList = fileNameList.subList(0, fileNumber);
            }

        }

        ExperimentResult result = new ExperimentResult();
        result.fileNumber = this.fileNumber;
        File file = new File(localResultPath, "Access.txt");
        if (!file.exists()) {
            file.createNewFile();
        }

        ExperimentResultItem resultItem = null;
        System.out.println("Starting the archive files Access test...");
        try (PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(file, true)))) {

//            System.out.println("->Access Files from HDFS");
//            resultItem = accessFromHDFS();
//            result.resultItems.add(resultItem);
//            printAccess(resultItem, writer);
//            System.out.println("->HAR : Access Files");
//            resultItem = accessFromHAR();
//            result.resultItems.add(resultItem);
//            printAccess(resultItem, writer);
//            System.out.println("->MapFile : Access Files");
//            resultItem = accessFromMapFile();
//            result.resultItems.add(resultItem);
//            printAccess(resultItem, writer);
            System.out.println("->HPF : Access Files");
            resultItem = accessFromHPF();
            result.resultItems.add(resultItem);
            printAccess(resultItem, writer);
            writer.println();
            writer.println();
        }
        return result;
    }

    public ExperimentResultItem creatHAR() throws IOException, Exception {
        //delete data
        Path harPath = new Path(harFilePath);
        if (fs.exists(harPath)) {
            fs.delete(harPath, true);
        }

        HadoopArchives har = new HadoopArchives(conf);

        String[] args = new String[6];
        args[0] = "-archiveName";
        args[1] = "har-" + fileNumber + ".har";
        args[2] = "-p";
        args[3] = hdfsFilesPath;

        args[4] = "./*";
        args[5] = hdfsArchivesDataPath;

        long currentTimeMillis = System.currentTimeMillis();
        int ret = ToolRunner.run(har, args);
        assertTrue("failed test", ret == 0);

        long time = System.currentTimeMillis() - currentTimeMillis;

        ExperimentResultItem resultItem = new ExperimentResultItem();
        resultItem.methodname = "HAR";
        resultItem.duration = time;
        resultItem.metadataUsage = calculateNameNodeMetadataUsage(harPath);

        System.gc();
        return resultItem;

    }

    public ExperimentResultItem uploadToHDFS() throws IOException {

        Path outPutDir = new Path(hdfsFilesPath);

        long currentTimeMillis = System.currentTimeMillis();

        Arrays.asList(lfs.listStatus(new Path(localDataPath))).parallelStream()
                .forEach(status -> {
                    try {
                        Path p = new Path(outPutDir, status.getPath().getName());

                        if (fs.exists(p)) {
                            FileStatus rs = fs.getFileStatus(p);

                            if (rs.isFile() && rs.getLen() == 0) {
                                fs.delete(p, true);
                            } else {
                                return;
                            }
                        }

                        fs.copyFromLocalFile(false, false, status.getPath(), p);

                    } catch (IOException ex) {
                        Logger.getLogger(PaperTestsHolder.class.getName()).log(Level.SEVERE, null, ex);
                    }
                });

        long time = System.currentTimeMillis() - currentTimeMillis;

        ExperimentResultItem resultItem = new ExperimentResultItem();
        resultItem.methodname = "UploadToHDFS";
        resultItem.duration = time;
        resultItem.metadataUsage = calculateNameNodeMetadataUsage(new Path(hdfsFilesPath));
        System.gc();
        return resultItem;
    }

    public ExperimentResultItem distUploadToHDFS() throws IOException, Exception {

        long currentTimeMillis = System.currentTimeMillis();

        DistCpOptions dco = new DistCpOptions(new Path(localDataPath), new Path(hdfsFilesPath));

        int result = ToolRunner.run(new DistCp(conf, dco), new String[]{"-m", "200", localDataPath, hdfsFilesPath});

        // assertEquals("Should have failed with a -1, indicating invalid arguments",-1, result);
        long time = System.currentTimeMillis() - currentTimeMillis;

        ExperimentResultItem resultItem = new ExperimentResultItem();
        resultItem.methodname = "DistCp UploadToHDFS";
        resultItem.duration = time;
        resultItem.metadataUsage = calculateNameNodeMetadataUsage(new Path(hdfsFilesPath));
        System.gc();
        return resultItem;
    }

    public ExperimentResultItem creatMapFile() throws IOException {

        Path path = new Path(mapFilePath);

        if (fs.exists(path)) {
            fs.delete(path, true);
        }

        MapFile.Writer.Option wKeyOpt = MapFile.Writer.keyClass(Text.class);
        SequenceFile.Writer.Option wValueOpt = SequenceFile.Writer.valueClass(BytesWritable.class);

        long currentTimeMillis = System.currentTimeMillis();

        try (MapFile.Writer writer = new MapFile.Writer(conf, path, wKeyOpt, wValueOpt)) {

            List<FileStatus> fses = Arrays.asList(lfs.listStatus(new Path(localDataPath)));
            fses.sort((x, y) -> x.getPath().getName().compareTo(y.getPath().getName()));
            for (FileStatus status : fses) {
                if (status.isFile()) {
                    byte[] bs = new byte[(int) status.getLen()];
                    lfs.open(status.getPath()).readFully(bs);
                    writer.append(new Text(status.getPath().getName()), new BytesWritable(bs));
                }
            }
        }

        long time = System.currentTimeMillis() - currentTimeMillis;

        ExperimentResultItem resultItem = new ExperimentResultItem();
        resultItem.methodname = "MapFile";
        resultItem.duration = time;
        resultItem.metadataUsage = calculateNameNodeMetadataUsage(path);
        System.gc();
        return resultItem;

    }

    public ExperimentResultItem creatHPF() throws IOException, Exception {
        Path path = new Path(hpFilePath);

        if (fs.exists(path)) {
            fs.delete(path, true);
        }

        long currentTimeMillis = System.currentTimeMillis();

        try (PerfectFile.Writer writer = new PerfectFile.Writer(conf, path, 200000, 1)) {
            for (FileStatus status : lfs.listStatus(new Path(localDataPath))) {
                writer.putFromLocal(status);
            }
        }

        long time = System.currentTimeMillis() - currentTimeMillis;

        ExperimentResultItem resultItem = new ExperimentResultItem();
        resultItem.methodname = "HPF";
        resultItem.duration = time;
        resultItem.metadataUsage = calculateNameNodeMetadataUsage(path);
        System.gc();
        return resultItem;

    }

    public ExperimentResultItem accessFromHAR() throws IOException, Exception {

        long currentTimeMillis = System.currentTimeMillis();
        long totalMemory = Runtime.getRuntime().totalMemory();

        Path parent = new Path("har:///" + harFilePath);

        FileSystem hfs = parent.getFileSystem(conf);

        for (String name : fileNameSubList) {

            Path harSfPath = new Path(parent, (this.fileNumber >= 300000 ? "files/" : "") + name);

            FileStatus status = hfs.getFileStatus(harSfPath);
            byte[] bs = new byte[(int) status.getLen()];

            try (FSDataInputStream in = hfs.open(harSfPath)) {
                in.readFully(bs);
            }

            if (!cache) {
                hfs = HarFileSystem.get(parent.toUri(), conf);
            }
        }
        long time = System.currentTimeMillis() - currentTimeMillis;

        ExperimentResultItem resultItem = new ExperimentResultItem();
        resultItem.methodname = "HAR";
        resultItem.duration = time;
        resultItem.metadataUsage = Runtime.getRuntime().totalMemory() - totalMemory;

        System.gc();
        return resultItem;

    }

    public ExperimentResultItem accessFromHDFS() throws IOException {

        long totalMemory = Runtime.getRuntime().totalMemory();

        long currentTimeMillis = System.currentTimeMillis();

        FileSystem fs = FileSystem.get(conf);

        for (String name : fileNameSubList) {
            byte[] bs;
            try {
                try (FSDataInputStream in = fs.open(new Path(hdfsFilesPath, name))) {
                    bs = new byte[in.available()];
                    in.readFully(bs);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }

            if (!cache) {
                fs = FileSystem.get(conf);
            }
        }

        long time = System.currentTimeMillis() - currentTimeMillis;

        ExperimentResultItem resultItem = new ExperimentResultItem();
        resultItem.methodname = "FromHDFS";
        resultItem.metadataUsage = Runtime.getRuntime().totalMemory() - totalMemory;
        resultItem.duration = time;

        System.gc();
        return resultItem;
    }

    public ExperimentResultItem accessFromMapFile() throws IOException {

        long totalMemory = Runtime.getRuntime().totalMemory();

        long currentTimeMillis = System.currentTimeMillis();

        if (cache) {
            try (MapFile.Reader mReader = new MapFile.Reader(new Path(mapFilePath), conf)) {
                for (String name : fileNameSubList) {
                    BytesWritable bw = new BytesWritable();
                    mReader.get(new Text(name), bw);
                    bw.getBytes();
                }
            }
        } else {
            for (String name : fileNameSubList) {
                try (MapFile.Reader mReader = new MapFile.Reader(new Path(mapFilePath), conf)) {
                    BytesWritable bw = new BytesWritable();
                    mReader.get(new Text(name), bw);
                    bw.getBytes();
                }
            }
        }

        long time = System.currentTimeMillis() - currentTimeMillis;

        ExperimentResultItem resultItem = new ExperimentResultItem();
        resultItem.methodname = "MapFile";
        resultItem.metadataUsage = Runtime.getRuntime().totalMemory() - totalMemory;
        resultItem.duration = time;

        System.gc();
        return resultItem;
    }

    public ExperimentResultItem accessFromHPF() throws IOException, Exception {
        long totalMemory = Runtime.getRuntime().totalMemory();

        long currentTimeMillis = System.currentTimeMillis();

        try (PerfectFile.Reader reader = new PerfectFile.Reader(conf, new Path(hpFilePath))) {
            for (String name : fileNameSubList) {
                reader.getBytes(name);
            }
        }

        long time = System.currentTimeMillis() - currentTimeMillis;

        ExperimentResultItem resultItem = new ExperimentResultItem();
        resultItem.methodname = "HPF";
        resultItem.metadataUsage = Runtime.getRuntime().totalMemory() - totalMemory;
        resultItem.duration = time;

        System.gc();
        return resultItem;
    }

    private void printLog(ExperimentResultItem resultItem, PrintWriter writer, boolean creat) {
        writer.println("-->" + resultItem.methodname);
        writer.println("---->Duration(ms) : " + resultItem.duration);
        if (creat) {
            writer.println("---->NameNodeMetadataUsage(bytes) : " + resultItem.metadataUsage);
        } else {
            writer.println("---->Client MetadataUsage(bytes) : " + resultItem.metadataUsage);
        }
        writer.println();
        System.out.println(resultItem);
    }

    private void printAccess(ExperimentResultItem resultItem, PrintWriter writer) {
        writer.println("-->" + resultItem.methodname);
        writer.println("-->Cache enabled" + cache);
        writer.println("---->Duration(ms) : " + resultItem.duration);
        writer.println("---->Client MetadataUsage(bytes) : " + resultItem.metadataUsage);
        writer.println();
        System.out.println(resultItem);
    }

    /**
     * returns the amount of memory occupied by the contents(files,folders) of
     * the folder within NameNode
     *
     * @param dirPath
     * @return
     * @throws IOException
     */
    public long calculateNameNodeMetadataUsage(Path dirPath) throws IOException {
        return Arrays.asList(fs.listStatus(dirPath)).parallelStream().mapToLong(m -> calculateNameNodeMetadataUsage(m)).sum();
    }

    public long calculateNameNodeMetadataUsageForHAR(Path dirPath) throws IOException {
        return Arrays.asList(fs.listStatus(dirPath)).parallelStream().mapToLong(m -> calculateNameNodeMetadataUsage(m)).sum();
    }

    public long calculatePathSize(Path dirPath) throws IOException {
        return fs.getContentSummary(dirPath).getLength();
    }

    /**
     * returns the amount of memory occupied by files/folders within NameNode
     *
     * @see https://issues.apache.org/jira/browse/HADOOP-1687
     * @param status
     * @return
     * @throws IOException
     */
    private long calculateNameNodeMetadataUsage(FileStatus status) {
        if (status.isDirectory()) {
//            return 264 + 2 * fileName.length;
            return 290;
        } else {
            try {
                int bocksNum = fs.getFileBlockLocations(status, 0, status.getLen()).length;
                return 250 + bocksNum * 150 + (bocksNum * status.getReplication() * 72);
            } catch (IOException ex) {
                Logger.getLogger(PaperTestsHolder.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        return 0;
    }

    public long calculateNameNodeMetadataUsageBlock(int bocksNum) {
        return 250 + bocksNum * 150 + (bocksNum * 3 * 72);
    }

}
