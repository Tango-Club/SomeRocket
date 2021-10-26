package io.openmessaging;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

public class MessageBuffer {
	private static final Logger logger = Logger.getLogger(MessageBuffer.class);
	final ConcurrentHashMap<Integer, StorageEngine> cacheMap = new ConcurrentHashMap<>();
	final String topic;

	MessageBuffer(String topic) {
		this.topic = topic;
	}

	private void createStorage(int queueId) {
		if (!cacheMap.containsKey(queueId)) {
			int rd = queueId % 100;
			if (rd <= 20) {
				String cachePath = Common.runDir + "/essd/cache";
				cacheMap.put(queueId, new StorageEngineEssd(topic, queueId, cachePath));
			} else if (rd <= 23) {
				cacheMap.put(queueId, new StorageEngineDdr(false));
			} else if (rd <= 23) {
				cacheMap.put(queueId, new StorageEngineDdr(true));
			} else {
				cacheMap.put(queueId, new StorageEnginePmem());
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