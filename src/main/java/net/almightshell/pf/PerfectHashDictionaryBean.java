/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.almightshell.pf;

import it.unimi.dsi.sux4j.mph.HollowTrieMonotoneMinimalPerfectHashFunction;
import java.io.Serializable;
import java.util.HashMap;

/**
 *
 * @author Shell
 */
public class PerfectHashDictionaryBean implements Serializable {

    private HollowTrieMonotoneMinimalPerfectHashFunction function;

    public PerfectHashDictionaryBean() {
    }

    public PerfectHashDictionaryBean(HollowTrieMonotoneMinimalPerfectHashFunction function) {
        this.function = function;
    }

    public HollowTrieMonotoneMinimalPerfectHashFunction getFunction() {
        return function;
    }

    public void setFunction(HollowTrieMonotoneMinimalPerfectHashFunction function) {
        this.function = function;
    }

}
