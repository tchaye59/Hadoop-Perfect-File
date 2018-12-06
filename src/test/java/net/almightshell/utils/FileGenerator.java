/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.almightshell.utils;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.UUID;

/**
 *
 * @author Shell
 */
public class FileGenerator {

    long maxSize = 412 * 1024 * 1024l;
    String outPutDir;
    int fileNumber;

    public FileGenerator(long maxSize, String outPutDir, int fileNumber) {
        this.maxSize = maxSize;
        this.outPutDir = outPutDir;
        this.fileNumber = fileNumber;
    }

    public void generate() throws IOException {
        File dir = new File(outPutDir, "data-" + fileNumber);
        if (!dir.exists()) {
            dir.mkdirs();
        }
        

        for (int i = 0; i < 10; i++) {
            File f = new File(dir, UUID.randomUUID().toString());
            if (f.exists()) {
                continue;
            }
            f.createNewFile();

            int size = (int) (maxSize * Math.random());

            PrintWriter pw = new PrintWriter(f);

            while (f.length() <= size) {
                String s = UUID.randomUUID().toString();
                s =s+s+s+s+s+s+s+s+s+s+s+s+s+s+s+s+s+s; 
                pw.println(s);
            }
        }
    }
}
