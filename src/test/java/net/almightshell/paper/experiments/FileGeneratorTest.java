/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.almightshell.paper.experiments;

import java.io.IOException;
import net.almightshell.utils.FileGenerator;

/**
 *
 * @author Shell
 */
public class FileGeneratorTest {

    @org.junit.Test
    public void generate() throws IOException {
        FileGenerator generator = new FileGenerator((long) (.1 * 1024 * 1024l), "E:\\hadoop-experiment\\data", 100000);
        generator.generate();
    }
    
}
