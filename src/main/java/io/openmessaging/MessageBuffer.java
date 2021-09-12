package io.openmessaging;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import io.openmessaging.LlplCache;
import io.openmessaging.DiskStorage;

public class MessageBuffer {
	DiskStorage storage;
	LlplCache cache;
	String topic;
	int queueId;

	private boolean checkHot() {
		// TODO: Complete the hot algorithm
		return false;
	}

	private void initPath(String path) {
		File file = new File(path);
		if (file.exists())
			file.delete();
		try {
			file.createNewFile();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	MessageBuffer(String topic, int queueId) throws FileNotFoundException {
		this.topic = topic;
		this.queueId = queueId;

		String storagePath = "./ds_" + this.topic + "_" + Integer.toString(this.queueId);
		String cachePath = "./lc_" + this.topic + "_" + Integer.toString(this.queueId);
		initPath(storagePath);
		initPath(cachePath);

		storage = new DiskStorage(new FileReader(storagePath));
		cache = new LlplCache(new FileReader(cachePath));
	}

	public long add(ByteBuffer data) {
		int pos = storage.writeToDisk(data);
		if (checkHot())
			cache.writeToDisk(data);
		return pos;
	}

	public HashMap<Integer, ByteBuffer> get(long offset, int fetchNum) {
		if (cache.inLlpl(offset, fetchNum)) {
			return cache.readFromDisk(offset, fetchNum);
		} else {
			return storage.readFromDisk(offset, fetchNum);
		}
	}
}
