package io.openmessaging;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.log4j.Logger;

public class MessageBuffer {
	final ConcurrentHashMap<Integer, StorageEngine> cacheMap = new ConcurrentHashMap<>();
	final String topic;

	private static final Logger logger = Logger.getLogger(MessageBuffer.class);

	private void createStorage(int queueId) {
		if (!cacheMap.containsKey(queueId)) {
			try {
				String cachePath;
				if ((queueId % 3) != 0)
					cachePath = Common.runDir + "/essd/cache";
				else
					cachePath = Common.runDir + "/pmem/cache";

				cacheMap.put(queueId, new StorageEngine(topic, queueId, cachePath));
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	MessageBuffer(String topic) {
		this.topic = topic;
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
