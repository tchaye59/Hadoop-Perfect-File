/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.almightshell.efiles;

import eu.danieldk.dictomaton.PerfectHashDictionary;
import java.io.Serializable;
import java.util.HashMap;

/**
 *
 * @author Shell
 */
public class PerfectHashDictionaryBean implements Serializable {

    private HashMap<String, PerfectHashDictionary> map = new HashMap<>();

    public PerfectHashDictionaryBean() {
    }

    public PerfectHashDictionaryBean(HashMap<String, PerfectHashDictionary> map) {
        this.map = map;
    }

    public HashMap<String, PerfectHashDictionary> getMap() {
        return map;
    }

    public void setMap(HashMap<String, PerfectHashDictionary> map) {
        this.map = map;
    }

}
