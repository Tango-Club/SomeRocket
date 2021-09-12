package io.openmessaging;

import java.io.FileNotFoundException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import io.openmessaging.MessageBuffer;

public class DefaultMessageQueueImpl extends MessageQueue {
	ConcurrentHashMap<String, HashMap<Integer, MessageBuffer>> topicQueueMap = new ConcurrentHashMap<>();
	ConcurrentHashMap<String, Map<Integer, Map<Long, ByteBuffer>>> appendData = new ConcurrentHashMap<>();

	// getOrPutDefault 若指定key不存在，则插入defaultValue并返回
	private <K, V> V getOrPutDefault(Map<K, V> map, K key, V defaultValue) {
		V retObj = map.get(key);
		if (retObj != null) {
			return retObj;
		}
		map.put(key, defaultValue);
		return defaultValue;
	}

	@Override
	public long append(String topic, int queueId, ByteBuffer data) throws FileNotFoundException {
		if (!topicQueueMap.containsKey(topic)) {
			topicQueueMap.put(topic, new HashMap<Integer, MessageBuffer>());
		}
		if (!topicQueueMap.get(topic).containsKey(queueId)) {
			topicQueueMap.get(topic).put(queueId, new MessageBuffer(topic, queueId));
		}
		return topicQueueMap.get(topic).get(queueId).add(data);

	}

	@Override
	public Map<Integer, ByteBuffer> getRange(String topic, int queueId, long offset, int fetchNum) {

		if (!topicQueueMap.containsKey(topic)) {
			return new HashMap<>();
		}
		if (!topicQueueMap.get(topic).containsKey(queueId)) {
			return new HashMap<>();
		}
		return topicQueueMap.get(topic).get(queueId).get(offset, fetchNum);
	}
}
