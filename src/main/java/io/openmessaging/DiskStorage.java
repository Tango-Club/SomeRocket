package io.openmessaging;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.StringTokenizer;
import io.openmessaging.FastScanner;

public class DiskStorage {
	FastScanner scanner;
	FastWriter writer;

	DiskStorage(FileReader fd) {
		scanner = new FastScanner(fd);
		writer = new FastWriter();
	}

	int writeToDisk(ByteBuffer data) {

		byte[] b = new byte[data.remaining()];
		return writer.write(b);
		// TODO: WRITE TO DISK

	}

	HashMap<Integer, ByteBuffer> readFromDisk(long offset, int num) {
		// TODO: GET FROM DISK
		byte[] bytes = new byte[500];
		HashMap<Integer, ByteBuffer> ret = new HashMap<>();
		ret.put(1, ByteBuffer.wrap(bytes));
		return ret;
	}

}
