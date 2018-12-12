/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.almightshell.paper.experiments;

import it.unimi.dsi.bits.BitVector;
import it.unimi.dsi.bits.LongArrayBitVector;
import it.unimi.dsi.bits.TransformationStrategies;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.sux4j.mph.HollowTrieDistributorMonotoneMinimalPerfectHashFunction;
import it.unimi.dsi.sux4j.mph.HollowTrieMonotoneMinimalPerfectHashFunction;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import static junit.framework.Assert.assertEquals;

/**
 *
 * @author Shell
 */
public class Test {

    @org.junit.Test
    public void test() throws IOException {

        Long[] keys = {100L, 1L, -89L, 96541L, 485L, 495487L, 8959598565L, 0L,};

        Arrays.sort(keys, (Long o1, Long o2) -> Test.compare(o1, o2));

        final HollowTrieMonotoneMinimalPerfectHashFunction<BitVector> htmmphf = new HollowTrieMonotoneMinimalPerfectHashFunction(listOf(keys).iterator(), TransformationStrategies.identity());
         
        for (long key : keys) {
            System.out.println(key + "----->" + htmmphf.getLong(toBitVector(key)));
        }
        System.out.println("----->" + htmmphf.getLong(toBitVector(5484L)));

        HollowTrieMonotoneMinimalPerfectHashFunction<BitVector> hollowTrie = new HollowTrieMonotoneMinimalPerfectHashFunction<>(
                listOf(new int[][]{{0, 0, 0, 0, 0}, {0, 1, 0, 0, 0}, {0, 1, 0, 1, 0, 0}, {0, 1, 0, 1, 0, 1}, {0, 1, 1, 1, 0}}).iterator(), 
                TransformationStrategies.identity());

        assertEquals(0, hollowTrie.getLong(LongArrayBitVector.of(0, 0, 0, 0, 0)));
        assertEquals(1, hollowTrie.getLong(LongArrayBitVector.of(0, 1, 0, 0, 0)));
        assertEquals(2, hollowTrie.getLong(LongArrayBitVector.of(0, 1, 0, 1, 0, 0)));
        assertEquals(3, hollowTrie.getLong(LongArrayBitVector.of(0, 1, 0, 1, 0, 1)));
        assertEquals(4, hollowTrie.getLong(LongArrayBitVector.of(0, 1, 1, 1, 0)));
        assertEquals(5, hollowTrie.size64());
        System.out.println("dwdd : "+hollowTrie.getLong(LongArrayBitVector.of(1, 1, 1, 1, 1)));
        
    }

    public static ObjectArrayList<BitVector> listOf(final Long... keys) {
        final ObjectArrayList<BitVector> vectors = new ObjectArrayList<>();

        for (long key : keys) {
            vectors.add(toBitVector(key));
        }

        return vectors;
    }

    public static LongArrayBitVector toBitVector(Long key) {
        return LongArrayBitVector.wrap(new long[]{key});
    }

    public static ObjectArrayList<BitVector> listOf(final int[]... bit) {
        final ObjectArrayList<BitVector> vectors = new ObjectArrayList<>();
        for (final int[] v : bit) {
            vectors.add(LongArrayBitVector.of(v));
        }
        return vectors;
    }

    public static int compare(long x, long y) {
        return toBitVector(x).compareTo(toBitVector(y));
    }
}
