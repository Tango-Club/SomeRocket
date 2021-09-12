package io.openmessaging;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.HashMap;

public class DiskStorage {
	StorageEngine engine;

	DiskStorage(String topic, int queueId) throws IOException {
		String pathPre = "./storage/ds_" + topic + "_" + Integer.toString(queueId);

		String dataPath = pathPre + ".data";
		initPath(dataPath);

		String offsetPath = pathPre + ".offset";
		boolean exist = initPath(offsetPath);

		engine = new StorageEngine(dataPath, offsetPath, exist);
	}

	private void initDirectory() {
		File file = new File("./storage");
		if (!file.exists()) {
			file.mkdir();
		}
	}

	private boolean initPath(String path) {
		initDirectory();
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
