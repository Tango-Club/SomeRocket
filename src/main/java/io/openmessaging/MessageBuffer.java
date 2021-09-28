package io.openmessaging;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

public class MessageBuffer {
	private static final Logger logger = Logger.getLogger(MessageBuffer.class);
	final ConcurrentHashMap<Integer, StorageEngineEssd> cacheMap = new ConcurrentHashMap<>();
	final String topic;

	MessageBuffer(String topic) {
		this.topic = topic;
	}

	private void createStorage(int queueId) {
		if (!cacheMap.containsKey(queueId)) {
			String cachePath;
			if ((queueId % 100) < 24) {
				cachePath = Common.runDir + "/essd/cache";
				cacheMap.put(queueId, new StorageEngineEssd(topic, queueId, cachePath));
			} else {
				cachePath = Common.runDir + "/pmem/cache";
				cacheMap.put(queueId, new StorageEngineEssd(topic, queueId, cachePath));
			}
		}
	}

	public long appendData(int queueId, ByteBuffer data) throws IOException {
		createStorage(queueId);
		return cacheMap.get(queueId).write(data);
	}

	public HashMap<Integer, ByteBuffer> getRange(int queueId, long offset, int fetchNum) {
		createStorage(queueId);
		return cacheMap.get(queueId).getRange(offset, fetchNum);
	}
}
