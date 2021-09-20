package io.openmessaging;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.log4j.Logger;

public class MessageBuffer {
	ConcurrentHashMap<Integer, StorageEngine> queueMap = new ConcurrentHashMap<>();

	DiskStorage storage;
	DiskStorage cache;
	String topic;
	int queueId;
	boolean isReload;
	private static Logger logger = Logger.getLogger(MessageBuffer.class);

	private void creatStorage(int queueId) {
		if (!queueMap.containsKey(queueId)) {
			queueMap.put(queueId, new StorageEngine(topic));
		}
	}
	
	MessageBuffer(String topic) throws IOException {
		this.topic = topic;
		Common.initDirectory("/essd");
		Common.initDirectory("/pmem");
		String storagePath = "/essd/storage";
		String cachePath = "/pmem/cache";
		if (queueId % 3 != 0) {
			cachePath = "/essd/cache";
		}
		storage = new DiskStorage(topic, queueId, storagePath);
		cache = new DiskStorage(topic, queueId, cachePath);
		isReload = cache.engine.isReload();
	}

	public long appendData(ByteBuffer data) throws IOException, InterruptedException {
		if (isReload) {
			return storage.writeToDisk(data);
		}
		storage.writeToDisk(data);
		data.position(0);
		return cache.writeToDisk(data);
	}

	public HashMap<Integer, ByteBuffer> getRange(long offset, int fetchNum) {
		if (isReload) {
			return storage.readFromDisk(offset, fetchNum);
		}
		return cache.readFromDisk(offset, fetchNum);
	}
}
