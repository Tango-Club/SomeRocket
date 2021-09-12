package io.openmessaging;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import io.openmessaging.LlplCache;
import io.openmessaging.DiskStorage;


public class MessageBuffer {
    DiskStorage ds;
    LlplCache lc;
    String topic;
    int queueId;

    private boolean checkHot(){
        //TODO: Complete the hot algorithm
        return false;
    }
    MessageBuffer(String topic,int queueId) throws FileNotFoundException {
        this.topic=topic;
        this.queueId=queueId;
        ds=new DiskStorage(new FileReader("./ds_"+this.topic+"_"+Integer.toString(this.queueId)));
        lc=new LlplCache(new FileReader("./lc_"+this.topic+"_"+Integer.toString(this.queueId)));
    }

    public long add(ByteBuffer data){
        int pos = ds.writeToDisk(data);
        if(checkHot())
            lc.writeToDisk(data);
        return pos;
    }
    public HashMap<Integer, ByteBuffer> get(long offset, int fetchNum){
        if(lc.inLlpl(offset,fetchNum)){
            return lc.readFromDisk(offset,fetchNum);
        }
        else{
            return ds.readFromDisk(offset,fetchNum);
        }
    }
}
