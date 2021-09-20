package io.openmessaging;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.log4j.Logger;

public class MessageBuffer {
	ConcurrentHashMap<Integer, DiskStorage> cacheMap = new ConcurrentHashMap<>();

	StorageEngineSynced storage;
	String topic;
	String storagePath;
	String cachePath;

	boolean isReload;
	private static Logger logger = Logger.getLogger(MessageBuffer.class);

	private void creatStorage(int queueId) {
		if (!cacheMap.containsKey(queueId)) {
			try {
				String cachePath;
				if (queueId % 3 != 0)
					cachePath = "/essd/cache";
				else
					cachePath = "/pmem/cache";

				cacheMap.put(queueId, new DiskStorage(topic, queueId, cachePath));
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	MessageBuffer(String topic) throws IOException {
		this.topic = topic;
		Common.initDirectory("/essd");
		Common.initDirectory("/essd/storage");
		Common.initDirectory("/pmem");
		storagePath = "/essd/storage/sync_" + topic;
		isReload = !Common.initDirectory(storagePath);
		storage = new StorageEngineSynced(storagePath, isReload);
		if (isReload)
			recover();
	}

	private void recover() {
		Common.cleanPath("/essd/cache");
		Common.cleanPath("/pmem/cache");

		for (long i = 0; i < storage.dataNumber; i++) {
			int queueId = storage.getQidByIndex(i + 1);
			ByteBuffer buffer = storage.getDataByIndex(i);
			try {
				creatStorage(queueId);
				cacheMap.get(queueId).writeToDisk(buffer);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public long appendData(int queueId, ByteBuffer data) throws IOException, InterruptedException {
		storage.write(queueId, data);
		storage.flush();
		synchronized (this) {
			long tBefore = System.currentTimeMillis();
			wait(Common.syncTime);
			if (System.currentTimeMillis() - tBefore >= Common.syncTime) {
				storage.flush();
				notifyAll();
			}
		}
		data.position(0);
		creatStorage(queueId);
		return cacheMap.get(queueId).writeToDisk(data);
	}

	public HashMap<Integer, ByteBuffer> getRange(int queueId, long offset, int fetchNum) {
		creatStorage(queueId);
		return cacheMap.get(queueId).readFromDisk(offset, fetchNum);
	}
}
