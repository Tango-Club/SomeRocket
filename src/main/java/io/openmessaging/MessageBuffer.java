package io.openmessaging;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;

public class MessageBuffer {
	DiskStorage storage;
	DiskStorage cache;
	String topic;
	int queueId;
	boolean isReload;

	private boolean checkHot() {
		// TODO: Complete the hot algorithm
		return false;
	}

	MessageBuffer(String topic, int queueId) throws IOException {
		this.topic = topic;
		this.queueId = queueId;

		storage = new DiskStorage(topic, queueId, "/essd", true);
		cache = new DiskStorage(topic, queueId, "/essd/cache", false);
		isReload = cache.engine.isReload();
	}

	public long appendData(ByteBuffer data) throws IOException {
		if (isReload) {
			return storage.writeToDisk(data);
		}
		long pos = cache.writeToDisk(data);
		data.flip();
		CompletableFuture.runAsync(() -> {
			try {
				storage.writeToDisk(data);
			} catch (IOException e) {
				e.printStackTrace();
			}
		});
		return pos;
	}

	public HashMap<Integer, ByteBuffer> getRange(long offset, int fetchNum) {
		if (isReload) {
			return storage.readFromDisk(offset, fetchNum);
		} else {
			return cache.readFromDisk(offset, fetchNum);
		}
	}
}
