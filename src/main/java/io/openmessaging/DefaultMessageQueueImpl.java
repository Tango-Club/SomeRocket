package io.openmessaging;

import java.io.FileNotFoundException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import io.openmessaging.MessageBuffer;

/**
 * 这是一个简单的基于内存的实现，以方便选手理解题意；
 * 实际提交时，请维持包名和类名不变，把方法实现修改为自己的内容；
 */
public class DefaultMessageQueueImpl extends MessageQueue {
    ConcurrentHashMap<String, HashMap<Integer, MessageBuffer> > TopicQueueMap = new ConcurrentHashMap<>();
   // ConcurrentHashMap<String, Map<Integer, Map<Long, ByteBuffer>>> appendData = new ConcurrentHashMap<>();

    // getOrPutDefault 若指定key不存在，则插入defaultValue并返回
    /*private <K, V> V getOrPutDefault(Map<K, V> map, K key, V defaultValue){
        V retObj = map.get(key);
        if(retObj != null){
            return retObj;
        }
        map.put(key, defaultValue);
        return defaultValue;
    }*/

    @Override
    public long append(String topic, int queueId, ByteBuffer data) throws FileNotFoundException {
        if(!TopicQueueMap.containsKey(topic)){
            TopicQueueMap.put(topic,new HashMap<Integer, MessageBuffer>());
        }
        if(!TopicQueueMap.get(topic).containsKey(queueId)){
            TopicQueueMap.get(topic).put(queueId,new MessageBuffer(topic,queueId));
        }
        return TopicQueueMap.get(topic).get(queueId).add(data);

    }

    @Override
    public Map<Integer, ByteBuffer> getRange(String topic, int queueId, long offset, int fetchNum) {

        if(!TopicQueueMap.containsKey(topic)){
            return new HashMap<>();
        }
        if(!TopicQueueMap.get(topic).containsKey(queueId)){
            return new HashMap<>();
        }
        return TopicQueueMap.get(topic).get(queueId).get(offset,fetchNum);

/*
        Map<Integer, ByteBuffer> ret = new HashMap<>();
        for(int i = 0; i < fetchNum; i++){
            Map<Integer, Map<Long, ByteBuffer>> map1 = appendData.get(topic);
            if(map1 == null){
                break;
            }
            Map<Long, ByteBuffer> m2 = map1.get(queueId);
            if(m2 == null){
                break;
            }
            ByteBuffer buf = m2.get(offset+i);
            if(buf != null){
                // 返回前确保 ByteBuffer 的 remain 区域为完整答案
                buf.position(0);
                buf.limit(buf.capacity());
                ret.put(i,buf);
            }
        }*/

    }
}
