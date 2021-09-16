package io.openmessaging;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.HashMap;

public class DiskStorage {
	StorageEngine engine;

	DiskStorage(String topic, int queueId, String basePath, boolean isStorage) throws IOException {
		Common.initDirectory(basePath);
		String storagePath = basePath + "/ds_" + topic + "_" + Integer.toString(queueId);

		boolean exist = !Common.initDirectory(storagePath);

		engine = new StorageEngine(storagePath, exist, isStorage);
	}

	long writeToDisk(ByteBuffer data) throws IOException {
		return engine.write(data);
	}

	HashMap<Integer, ByteBuffer> readFromDisk(long offset, int fetchNum) {
		return engine.getRange(offset, fetchNum);
	}

}
