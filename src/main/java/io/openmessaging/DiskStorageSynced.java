package io.openmessaging;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.HashMap;

public class DiskStorageSynced {
	StorageEngine engine;

	DiskStorageSynced(String topic, String basePath) throws IOException {
		Common.initDirectory(basePath);
		String storagePath = basePath + "/sds_" + topic;

		boolean exist = !Common.initDirectory(storagePath);

		engine = new StorageEngine(storagePath, exist, isStorage);
	}

	long writeToDisk(ByteBuffer data, int queueId) throws IOException {
		return engine.write(data);
	}

	HashMap<Integer, ByteBuffer> readFromDisk(int queueId, long offset, int fetchNum) {
		return engine.getRange(offset, fetchNum);
	}

}
