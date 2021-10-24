package io.openmessaging;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;

public class MessageBuffer {
	private static final Logger logger = Logger.getLogger(MessageBuffer.class);
	final ArrayList<StorageEngine> cacheMap = new ArrayList<>();
	final String topic;

	MessageBuffer(String topic) {
		this.topic = topic;
		for (int i = 0; i <= 5000; i++) {
			int rd = i % 100;
			if (rd <= 19) {
				String cachePath = Common.runDir + "/essd/cache";
				cacheMap.add(new StorageEngineEssd(topic, i, cachePath));
			} else if (rd <= 23) {
				cacheMap.add(new StorageEngineDdr(false));
			} else {
				cacheMap.add(new StorageEnginePmem());
			}
		}
	}

	public long appendData(int queueId, ByteBuffer data) throws IOException {
		return cacheMap.get(queueId).write(data);
	}

	public HashMap<Integer, ByteBuffer> getRange(int queueId, long offset, int fetchNum) {
		return cacheMap.get(queueId).getRange(offset, fetchNum);
	}
}
