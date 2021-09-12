package io.openmessaging;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;

public class MessageBuffer {
	DiskStorage storage;
	LlplCache cache;
	String topic;
	int queueId;

	private boolean checkHot() {
		// TODO: Complete the hot algorithm
		return false;
	}

	MessageBuffer(String topic, int queueId) throws IOException {
		this.topic = topic;
		this.queueId = queueId;

		storage = new DiskStorage(topic, queueId);
		cache = new LlplCache(topic, queueId);
	}

	public long appendData(ByteBuffer data) throws IOException {
		long pos = storage.writeToDisk(data);
		if (checkHot())
			cache.writeToDisk(data);
		return pos;
	}

	public HashMap<Integer, ByteBuffer> getRange(long offset, int fetchNum) {
		if (cache.inLlpl(offset, fetchNum)) {
			return cache.readFromDisk(offset, fetchNum);
		} else {
			return storage.readFromDisk(offset, fetchNum);
		}
	}
}
