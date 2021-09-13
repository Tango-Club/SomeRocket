package io.openmessaging;

import java.io.IOException;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.log4j.Logger;

public class DefaultMessageQueueImpl extends MessageQueue {
	private static Logger logger = Logger.getLogger(StorageEngine.class);
	ConcurrentHashMap<String, HashMap<Integer, MessageBuffer>> topicQueueMap = new ConcurrentHashMap<>();
	ConcurrentHashMap<String, Map<Integer, Map<Long, ByteBuffer>>> appendData = new ConcurrentHashMap<>();

	@Override
	public long append(String topic, int queueId, ByteBuffer data) {
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
		try {
			return topicQueueMap.get(topic).get(queueId).appendData(data);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return -1;
	}

	@Override
	public Map<Integer, ByteBuffer> getRange(String topic, int queueId, long offset, int fetchNum) {
		if (!topicQueueMap.containsKey(topic)) {
			return new HashMap<>();
		}
		if (!topicQueueMap.get(topic).containsKey(queueId)) {
			return new HashMap<>();
		}
		return topicQueueMap.get(topic).get(queueId).getRange(offset, fetchNum);
	}
}
