package io.openmessaging;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.log4j.Logger;

public class DefaultMessageQueueImpl extends MessageQueue {
	private static Logger logger = Logger.getLogger(StorageEngine.class);
	ConcurrentHashMap<String, MessageBuffer> topicQueueMap = new ConcurrentHashMap<>();

	private void creatStorage(String topic) {
		if (!topicQueueMap.containsKey(topic)) {
			try {
				topicQueueMap.put(topic, new MessageBuffer(topic));
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	@Override
	public long append(String topic, int queueId, ByteBuffer data) {
		creatStorage(topic);
		try {
			return topicQueueMap.get(topic).appendData(queueId, data);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return -1;
	}

	@Override
	public Map<Integer, ByteBuffer> getRange(String topic, int queueId, long offset, int fetchNum) {
		creatStorage(topic);
		return topicQueueMap.get(topic).getRange(queueId, offset, fetchNum);
	}
}
