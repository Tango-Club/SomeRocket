package io.openmessaging;

import java.nio.ByteBuffer;
import java.util.HashMap;

public class LlplCache {

	LlplCache(String topic, int queueId) {

	}

	int writeToDisk(ByteBuffer data) {
		return 0;
	}

	boolean inLlpl(long offset, int num) {
		return false;
	}

	HashMap<Integer, ByteBuffer> readFromDisk(long offset, int num) {
		// TODO: GET FROM DISK
		byte[] bytes = new byte[500];
		HashMap<Integer, ByteBuffer> ret = new HashMap<>();
		ret.put(1, ByteBuffer.wrap(bytes));
		return ret;
	}

}
