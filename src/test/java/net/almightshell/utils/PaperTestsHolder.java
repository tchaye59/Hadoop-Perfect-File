/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.almightshell.utils;

import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;
import static junit.framework.Assert.assertTrue;
import net.almightshell.pf.PerfectFile;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.HarFileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.tools.HadoopArchives;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author Shell
 */
public class PaperTestsHolder {

    private List<String> getDatasetsNames(int dataset) {
        List<String> names = new ArrayList<>();
        try {
            try (FSDataInputStream in = fs.open(new Path("/data/10000", "dataset-" + dataset))) {
                new IntWritable().readFields(in);
                while (in.available() > 0) {
                    names.add(Text.readString(in));
                }
            }
        } catch (Exception exception) {
        }
        return names;
    }

    public class ExperimentResult {

        public int fileNumber = 0;
        public List<ExperimentResultItem> resultItems = new ArrayList<>();
    }

    public class ExperimentResultItem {

        public String methodname = "";
        //size in byte
        public long nameNodeMetadataUsage = 0;

        public long duration = 0;

        @Override
        public String toString() {
            return "ExperimentResultItem{" + "methodname=" + methodname + ", nameNodeMetadataUsage=" + nameNodeMetadataUsage + ", duration=" + duration + '}';
        }

    }

    FileSystem fs;

    Configuration conf;

    String localDataPath = "E:\\hadoop-experiment\\data\\data-";
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

    private PerfectFile hpf = null;

    public PaperTestsHolder(FileSystem fs, Configuration conf, int fileNumber) throws IOException {
        this.fs = fs;
        this.conf = conf;
        this.fileNumber = fileNumber;
        this.localDataPath += fileNumber;
        this.localResultPath += fileNumber;
        this.hdfsDataPath += fileNumber;

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
        mapFilePath = hdfsArchivesDataPath + "/mapfile-" + fileNumber;
        hpFilePath = hdfsArchivesDataPath + "/hpf-" + fileNumber + ".hpf";
        hdfsFilesPath = hdfsDataPath + "/files";

        try {
            fileNameList = getDatasetsNames(fileNumber);
            if (fileNameList.isEmpty()) {
                for (FileStatus listStatu : fs.listStatus(new Path(hdfsFilesPath))) {
                    fileNameList.add(listStatu.getPath().getName());
                }
            }
            fileNameSubList = fileNameList;
        } catch (Exception e) {
            e.printStackTrace();
        }  

    }

    public ExperimentResult processUploadDataSets() throws FileNotFoundException, IOException, Exception {
        ExperimentResult result = new ExperimentResult();

        ExperimentResultItem resultItem = null;

        System.out.println("->Upload Files from HDFS");
        resultItem = uploadToHDFS();
        result.resultItems.add(resultItem);
        return result;
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

//            System.out.println("->HAR : Creation");
//            resultItem = creatHAR();
//            result.resultItems.add(resultItem);
//            printLog(resultItem, writer, true);

            System.out.println("->MapFile : Creation");
            resultItem = creatMapFile();
            result.resultItems.add(resultItem);
            printLog(resultItem, writer, true);

//            System.out.println("->HPF : Creation");
//            resultItem = creatHPF();
//            result.resultItems.add(resultItem);
//            printLog(resultItem, writer, true);

            writer.println();
            writer.println();
        }
        return result;
    }

    public ExperimentResult processAccess(int fileNumber) throws FileNotFoundException, IOException, Exception {
        return processAccess(fileNumber, true);
    }

    public ExperimentResult processAccess(int fileNumber, boolean random) throws FileNotFoundException, IOException, Exception {

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
        File file = new File(localResultPath, "Access.txt");
        if (!file.exists()) {
            file.createNewFile();
        }

        ExperimentResultItem resultItem = null;
        System.out.println("Starting the archive files Access test...");
        try (PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(file, true)))) {

            System.out.println("->Access Files from HDFS");
            resultItem = accessFromHDFS();
            result.resultItems.add(resultItem);
            printLog(resultItem, writer, false);

            System.out.println("->HAR : Access Files");
            resultItem = accessFromHAR();
            result.resultItems.add(resultItem);
            printLog(resultItem, writer, false);

//            System.out.println("->MapFile : Access Files");
//            resultItem = accessFromMapFile();
//            result.resultItems.add(resultItem);
//            printLog(resultItem, writer, false);
            System.out.println("->HPF : Access Files");
            resultItem = accessFromHPF();
            result.resultItems.add(resultItem);
            printLog(resultItem, writer, false);

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
        resultItem.nameNodeMetadataUsage = calculateNameNodeMetadataUsage(harPath);

        System.gc();
        return resultItem;

    }

    public ExperimentResultItem uploadToHDFS() throws IOException {

        Path outPutDir = new Path(hdfsFilesPath);

        long currentTimeMillis = System.currentTimeMillis();

//        RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(new Path(localDataPath),true);
//        
//        while (iterator.hasNext()) {
//            LocatedFileStatus next = iterator.next();
//             fs.copyFromLocalFile(false, false, status.getPath(), new Path(outPutDir, status.getPath().getName()));
//        }
        FileSystem lfs;
        lfs = new Path("file:///" + localDataPath).getFileSystem(conf);
        Arrays.asList(lfs.listStatus(new Path("file:///" + localDataPath))).parallelStream().forEach(status -> {
            try {
                Path dest = new Path(outPutDir, status.getPath().getName());
                if (!fs.exists(dest)) {
                    fs.copyFromLocalFile(false, false, status.getPath(), new Path(outPutDir, status.getPath().getName()));
                }
            } catch (Exception ex) {
                ex.printStackTrace();
                try {
                    Thread.sleep(100);
                } catch (InterruptedException ex1) {
                    Logger.getLogger(PaperTestsHolder.class.getName()).log(Level.SEVERE, null, ex1);
                }
            }
        });

//        for (FileStatus status : fs.listStatus(new Path(localDataPath))) {
//            fs.copyFromLocalFile(false, false, status.getPath(), new Path(outPutDir, status.getPath().getName()));
//        }
//        fs.copyFromLocalFile(new Path(localDataPath), outPutDir);
        long time = System.currentTimeMillis() - currentTimeMillis;

        ExperimentResultItem resultItem = new ExperimentResultItem();
        resultItem.methodname = "UploadToHDFS";
        resultItem.duration = time;
        resultItem.nameNodeMetadataUsage = calculateNameNodeMetadataUsage(new Path(hdfsFilesPath));
        System.gc();
        return resultItem;
    }

    public void generateDatasetsFiles(int[] datasets) throws IOException, InterruptedException {
        Collections.shuffle(fileNameList, new Random(System.nanoTime()));

        for (int dataset : datasets) {
            List<String> manes = fileNameList.subList(0, dataset);
            Path path = new Path(hdfsDataPath, "dataset-" + dataset);

            if (!fs.exists(path)) {
                try (FSDataOutputStream out = fs.create(path)) {
                    IntWritable iw = new IntWritable(manes.size());
                    iw.write(out);
                    for (String mane : manes) {
                        Text.writeString(out, mane);
                    }
                }
            } else {
                manes = getDatasetsNames(dataset);
            }

            Path tempDir = new Path("/data/", "" + dataset + "/files");
            if (!fs.exists(tempDir)) {
                fs.mkdirs(tempDir);
            }

            manes.parallelStream().forEach(mane -> {
                try {
                    Path destPath = new Path(tempDir, mane);

                    if (fs.exists(destPath)) {
                        FileStatus st = fs.getFileStatus(destPath);
                        if (st.getLen() > 0) {
                            return;
                        }
                        fs.delete(destPath, true);
                    }
                } catch (IOException ex) {
                    Logger.getLogger(PaperTestsHolder.class.getName()).log(Level.SEVERE, null, ex);
                }
            });

            manes.parallelStream().forEach(mane -> {

                try {
                    FileStatus srcStatus = fs.getFileStatus(new Path(hdfsFilesPath, mane));
                    Path destPath = new Path(tempDir, mane);

                    if (fs.exists(destPath)) {
                        FileStatus st = fs.getFileStatus(destPath);
                        if (st.getLen() > 0) {
                            return;
                        }
                        fs.delete(destPath, true);
                    }

                    try {
                        FSDataOutputStream out = fs.create(destPath);
                        IOUtils.copyBytes(fs.open(srcStatus.getPath()), out, conf);
                    } catch (IOException e) {
                        fs.delete(destPath, true);
                        e.printStackTrace();
                    }
                } catch (IOException ex) {
                    Logger.getLogger(PaperTestsHolder.class.getName()).log(Level.SEVERE, null, ex);
                }

            });

//            for (String mane : manes) {
//                FileStatus srcStatus = fs.getFileStatus(new Path(hdfsFilesPath, mane));
//                Path destPath = new Path(tempDir, mane);
//
//                if (!fs.exists(destPath)) {
//                    try {
//                        FSDataOutputStream out = fs.create(destPath);
//                        IOUtils.copyBytes(fs.open(srcStatus.getPath()), out, (int) srcStatus.getLen());
//                    } catch (IOException e) {
//                        Thread.sleep(100);
//                        fs.delete(destPath, false);
//                    }
//                }
//
//            }
        }
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
            List<FileStatus> fses = Arrays.asList(fs.listStatus(new Path(hdfsFilesPath)));
            fses.sort((x, y) -> x.getPath().getName().compareTo(y.getPath().getName()));
            for (FileStatus status : fses) {
                if (status.isFile()) {
                    byte[] bs = new byte[(int) status.getLen()];
                    fs.open(status.getPath()).readFully(bs);
                    writer.append(new Text(status.getPath().getName()), new BytesWritable(bs));
                }
            }
        }

        long time = System.currentTimeMillis() - currentTimeMillis;

        ExperimentResultItem resultItem = new ExperimentResultItem();
        resultItem.methodname = "MapFile";
        resultItem.duration = time;
        resultItem.nameNodeMetadataUsage = calculateNameNodeMetadataUsage(path);
        System.gc();
        return resultItem;

    }

    public ExperimentResultItem creatHPF() throws IOException, Exception {
        Path path = new Path(hpFilePath);

        if (fs.exists(path)) {
            fs.delete(path, true);
        }

        PerfectFile pf = PerfectFile.newFile(conf, path, 10000);

        long currentTimeMillis = System.currentTimeMillis();
        pf.putAll(new Path(hdfsFilesPath));
        long time = System.currentTimeMillis() - currentTimeMillis;

        ExperimentResultItem resultItem = new ExperimentResultItem();
        resultItem.methodname = "HPF";
        resultItem.duration = time;
        resultItem.nameNodeMetadataUsage = calculateNameNodeMetadataUsage(path);
        System.gc();
        return resultItem;

    }

    public ExperimentResultItem accessFromHAR() throws IOException, Exception {

        long currentTimeMillis = System.currentTimeMillis();

        Path parent = new Path("har:///" + harFilePath);
        FileSystem hfs = parent.getFileSystem(conf);

        for (String name : fileNameSubList) {
            Path harSfPath = new Path(parent, name);

            FileStatus status = hfs.getFileStatus(harSfPath);
            byte[] bs = new byte[(int) status.getLen()];

            try (FSDataInputStream in = hfs.open(harSfPath)) {
                in.readFully(bs);
            }
        }

        long time = System.currentTimeMillis() - currentTimeMillis;

        ExperimentResultItem resultItem = new ExperimentResultItem();
        resultItem.methodname = "HAR";
        resultItem.duration = time;

        System.gc();
        return resultItem;

    }

    public ExperimentResultItem accessFromHDFS() throws IOException {

        long currentTimeMillis = System.currentTimeMillis();
         
        for (String name : fileNameSubList) {
            byte[] bs ;
            try (FSDataInputStream in = fs.open(new Path(hdfsFilesPath, name))) {
                bs = new  byte[in.available()];
                in.readFully(bs);
            }
        }

        long time = System.currentTimeMillis() - currentTimeMillis;

        ExperimentResultItem resultItem = new ExperimentResultItem();
        resultItem.methodname = "FromHDFS";
        resultItem.duration = time;

        System.gc();
        return resultItem;
    }

    public ExperimentResultItem accessFromMapFile() throws IOException {
        
        long currentTimeMillis = System.currentTimeMillis();
        MapFile.Reader reader = new MapFile.Reader(new Path(mapFilePath), conf);

        for (String name : fileNameSubList) {
            BytesWritable bw = new BytesWritable();
            reader.get(new Text(name), bw);
            bw.getBytes();
        }

        long time = System.currentTimeMillis() - currentTimeMillis;

        ExperimentResultItem resultItem = new ExperimentResultItem();
        resultItem.methodname = "MapFile";
        resultItem.duration = time;

        System.gc();
        return resultItem;
    }

    public ExperimentResultItem accessFromHPF() throws IOException, Exception {

        if (hpf == null) {
            hpf = PerfectFile.open(conf, new Path(hpFilePath));
        }

        long currentTimeMillis = System.currentTimeMillis();
        for (String name : fileNameSubList) {
            hpf.getBytes(name);
        }
        long time = System.currentTimeMillis() - currentTimeMillis;

        ExperimentResultItem resultItem = new ExperimentResultItem();
        resultItem.methodname = "HPF";
        resultItem.duration = time;

        System.gc();
        return resultItem;
    }

    private void printLog(ExperimentResultItem resultItem, PrintWriter writer, boolean creat) {
        writer.println("-->" + resultItem.methodname);
        writer.println("---->Duration(ms) : " + resultItem.duration);
        if (creat) {
            writer.println("---->NameNodeMetadataUsage(bytes) : " + resultItem.nameNodeMetadataUsage);
        }
        writer.println();
        System.out.println(resultItem);
    }

    public void clean() throws IOException {
//            fs.delete(new Path(harFilePath), true);
//            fs.delete(new Path(mapFilePath), true);
//            fs.delete(new Path(hpFilePath), true);
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
        long size = 0;
        for (FileStatus status : fs.listStatus(dirPath)) {
            size += calculateNameNodeMetadataUsage(status);
        }
        return size;
    }

    /**
     * returns the amount of memory occupied by files/folders within NameNode
     *
     * @see https://issues.apache.org/jira/browse/HADOOP-1687
     * @param status
     * @return
     * @throws IOException
     */
    private long calculateNameNodeMetadataUsage(FileStatus status) throws IOException {
        if (status.isDirectory()) {
//            return 264 + 2 * fileName.length;
            return 290;
        } else {
            int bocksNum = fs.getFileBlockLocations(status, 0, status.getLen()).length;
            return 250 + bocksNum * 150 + (bocksNum * status.getReplication() * 72);
        }
    }

}
