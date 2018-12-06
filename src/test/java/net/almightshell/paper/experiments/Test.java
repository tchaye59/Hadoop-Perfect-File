/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.almightshell.paper.experiments;

import it.unimi.dsi.bits.TransformationStrategies;
import it.unimi.dsi.sux4j.mph.CHDMinimalPerfectHashFunction;
import it.unimi.dsi.sux4j.mph.GOVMinimalPerfectHashFunction;
import it.unimi.dsi.sux4j.mph.HollowTrieMonotoneMinimalPerfectHashFunction;
import it.unimi.dsi.sux4j.mph.TwoStepsGOV3Function;
import java.io.IOException;
import java.util.ArrayList; 
import java.util.List;

/**
 *
 * @author Shell
 */
public class Test {

    @org.junit.Test
    public void test() throws IOException {

        List<String> keys = new ArrayList<>();
        keys.add("100L");
        keys.add("1L");
        keys.add("-89L");
        keys.add("96541L");
        keys.add("485L");
        keys.add("495487L");
        keys.add("8959598565L");
        keys.add("0L");

        List<Long> keysl = new ArrayList<>();
        keysl.add(100L);
        keysl.add(1L);
        keysl.add(-89L);
        keysl.add(96541L);
        keysl.add(485L);
        keysl.add(495487L);
        keysl.add(8959598565L);
        keysl.add(0L);
        
        
        final GOVMinimalPerfectHashFunction<Long> mph = new GOVMinimalPerfectHashFunction.Builder<Long>().keys(keysl).transform(TransformationStrategies.fixedLong()).build();

        
        final CHDMinimalPerfectHashFunction<Long> f = new CHDMinimalPerfectHashFunction.Builder<Long>().keys(keysl).transform(TransformationStrategies.fixedLong()).build();
        
        keysl.forEach((key) -> {
            System.out.println(key + "--->" + mph.getLong(key));
        });
        
 
        
        

    }
}
