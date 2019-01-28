/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.almightshell.utils;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Shell
 */
public class FileGenerator {

    long maxSize = 412 * 1024 * 1024l;
    String outPutDir;
    int fileNumber;
    List<String> manes = null;
    
    public FileGenerator(long maxSize, String outPutDir, int fileNumber) {
        this.maxSize = maxSize;
        this.outPutDir = outPutDir;
        
        manes = new ArrayList<>(fileNumber);
        this.fileNumber = fileNumber;
        
        for (int i = 0; i < fileNumber; i++) {
            manes.add("file-"+i+".txt");
        }
        //s = s+s+s+s+s+s+s;
    }

    public void generate() throws IOException {
        File dir = new File(outPutDir, "data-" + fileNumber);
        if (!dir.exists()) {
            dir.mkdirs();
        }
        
        manes.parallelStream().forEach(mane->{
            try {
                File f = new File(dir,mane);
                if (f.exists()) {
                    return;
                }
                f.createNewFile();
                
                int size = (int) (maxSize * Math.random());
                
                PrintWriter pw = new PrintWriter(f);
                
                while (f.length() <= size) {
                    pw.println(UUID.randomUUID().toString());
                }
            } catch (IOException ex) {
                Logger.getLogger(FileGenerator.class.getName()).log(Level.SEVERE, null, ex);
            }
        });
        
    }
}
