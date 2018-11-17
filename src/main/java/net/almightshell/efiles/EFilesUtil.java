/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.almightshell.efiles;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.math.BigInteger;

/**
 *
 * @author Shell
 */
public class EFilesUtil {

    /**
     * Convert an Object object into stream of bytes.
     *
     * @param s java object.
     * @return stream of bytes
     */
    public static byte[] toObjectStream(Serializable s) {
        // Reference for stream of bytes
        byte[] stream = null;
        // ObjectOutputStream is used to convert a Java object into OutputStream
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                ObjectOutputStream oos = new ObjectOutputStream(baos);) {
            oos.writeObject(s);
            stream = baos.toByteArray();
        } catch (IOException e) {
            // Error in serialization
            e.printStackTrace();
        }
        return stream;
    }

    /**
     * Convert stream of bytes to Object.
     *
     * @param stream byte array
     * @return Student object
     */
    public static Serializable toObject(byte[] stream) {
        Serializable s = null;

        try (ByteArrayInputStream bais = new ByteArrayInputStream(stream);
                ObjectInputStream ois = new ObjectInputStream(bais);) {
            s = (Serializable) ois.readObject();
        } catch (IOException e) {
            // Error in de-serialization
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            // You are converting an invalid stream to Student
            e.printStackTrace();
        }
        return s;
    }

    public static String format(double bytes, int digits) {
        String[] dictionary = {"bytes", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB"};
        int index = 0;
        for (index = 0; index < dictionary.length; index++) {
            if (bytes < 1024) {
                break;
            }
            bytes = bytes / 1024;
        }
        return String.format("%." + digits + "f", bytes) + " " + dictionary[index];
    }

    public static int checkPositionInDirectory(long key, int globalDepth) {
        String s = Long.toBinaryString(key);
        s = s.substring(s.length() - globalDepth, s.length());
        return s.isEmpty() ? 0 : new BigInteger(s, 2).intValue();
    }

    public static int[] checkSplitPositionsInDirectory(long key, int globalDepth) {
        String s = Long.toBinaryString(key);
        s = s.substring(s.length() - globalDepth, s.length());

        StringBuilder sb = new StringBuilder(s);
        sb.setCharAt(0, '0');

        int x = new BigInteger(sb.toString(), 2).intValue();
        sb.setCharAt(0, '1');
        int y = new BigInteger(sb.toString(), 2).intValue();

        return new int[]{Math.min(x, y), Math.max(x, y)};
    }

}
