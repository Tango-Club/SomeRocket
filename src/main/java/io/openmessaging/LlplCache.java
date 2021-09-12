package io.openmessaging;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;

public class LlplCache {
    FastScanner sc;
    FastWriter wr;
    LlplCache(FileReader fd)  {
        sc = new FastScanner(fd);
        wr = new FastWriter();
    }
    int writeToDisk(ByteBuffer data){
        byte[] b = new byte[data.remaining()];
        return wr.write(b);
    }
    boolean inLlpl(long offset,int num){
        return false;
    }
    HashMap<Integer, ByteBuffer> readFromDisk(long offset, int num){
        //TODO: GET FROM DISK
        byte[] bytes = new byte [500];
        HashMap<Integer, ByteBuffer> ret= new HashMap<>();
        ret.put(1,ByteBuffer.wrap(bytes));
        return ret;
    }

}
