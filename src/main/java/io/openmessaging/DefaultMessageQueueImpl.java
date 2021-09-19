package io.openmessaging;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.log4j.Logger;

public class DefaultMessageQueueImpl extends MessageQueue {
	private static Logger logger = Logger.getLogger(StorageEngine.class);
	ConcurrentHashMap<String, HashMap<Integer, MessageBuffer>> topicQueueMap = new ConcurrentHashMap<>();
	ConcurrentHashMap<String, Map<Integer, Map<Long, ByteBuffer>>> appendData = new ConcurrentHashMap<>();
	Runtime runTime = Runtime.getRuntime();
	private void creatStorage(String topic, int queueId) {
		if (!topicQueueMap.containsKey(topic)) {
			topicQueueMap.put(topic, new HashMap<Integer, MessageBuffer>());
		}
		if (!topicQueueMap.get(topic).containsKey(queueId)) {
			try {
				topicQueueMap.get(topic).put(queueId, new MessageBuffer(topic, queueId));
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	@Override
	public long append(String topic, int queueId, ByteBuffer data) {
		creatStorage(topic, queueId);
		try {
			long result = topicQueueMap.get(topic).get(queueId).appendData(data);
			synchronized (this) {
				long tBefore = System.currentTimeMillis();
				wait(Common.syncTime);
				if (System.currentTimeMillis() - tBefore >= Common.syncTime) {
					runTime.exec("sync -d /essd");
					notifyAll();
				}
			}
			return result;
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return -1;
	}

	@Override
	public Map<Integer, ByteBuffer> getRange(String topic, int queueId, long offset, int fetchNum) {
		creatStorage(topic, queueId);
		return topicQueueMap.get(topic).get(queueId).getRange(offset, fetchNum);
	}
}
