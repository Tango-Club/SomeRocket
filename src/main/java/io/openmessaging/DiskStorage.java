package io.openmessaging;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.HashMap;

public class DiskStorage {
	StorageEngine engine;

	DiskStorage(String topic, int queueId, String basePath, boolean isStorage) throws IOException {
		initDirectory(basePath);
		String pathPre = basePath + "/ds_" + topic + "_" + Integer.toString(queueId);

		String dataPath = pathPre + ".data";
		initPath(dataPath);

		String offsetPath = pathPre + ".offset";
		boolean exist = initPath(offsetPath);

		engine = new StorageEngine(dataPath, offsetPath, exist, isStorage);
	}

	private void initDirectory(String basePath) {
		File file = new File(basePath);
		if (!file.exists()) {
			file.mkdir();
		}
	}

	private boolean initPath(String path) {
		File file = new File(path);
		try {
			if (!file.exists()) {
				file.createNewFile();
				return false;
			} else
				return true;
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
	}

	long writeToDisk(ByteBuffer data) throws IOException {
		return engine.write(data);
	}

	HashMap<Integer, ByteBuffer> readFromDisk(long offset, int fetchNum) {
		return engine.getRange(offset, fetchNum);
	}

}
