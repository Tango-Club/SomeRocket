package io.openmessaging;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.HashMap;
import org.apache.log4j.Logger;

public class StorageEngine {
	RandomAccessFile dataFile;
	RandomAccessFile offsetFile;
	long dataNumber;
	long lastOffset;

	private static Logger logger = Logger.getLogger(StorageEngine.class);

	private long getOffsetByIndex(long x) throws IOException {
		offsetFile.seek(x * 8);
		return offsetFile.readLong();
	}

	private void appendOffset(long offset) throws IOException {
		offsetFile.seek(offsetFile.length());
		offsetFile.writeLong(offset);
	}

	StorageEngine(String dataPath, String offsetPath, boolean exist) throws IOException {

		dataFile = new RandomAccessFile(dataPath, "rws");
		offsetFile = new RandomAccessFile(offsetPath, "rws");

		if (exist) {
			dataNumber = (long) (offsetFile.length() / 8) - 1;
			lastOffset = getOffsetByIndex(dataNumber);
		} else {
			dataNumber = 0;
			lastOffset = 0;
			appendOffset(0);
		}
		dataFile.seek(lastOffset);
	}

	public long write(ByteBuffer buffer) throws IOException {
		dataFile.seek(dataFile.length());
		byte[] data = new byte[buffer.capacity()];

		buffer.get(data);
		dataFile.write(data);

		lastOffset += buffer.capacity();
		appendOffset(lastOffset);
		dataNumber++;
		return dataNumber;
	}

	public ByteBuffer readNoSeek(long offset, int length) throws IOException {
		ByteBuffer buffer = ByteBuffer.allocate(length);
		byte[] data = new byte[length];
		dataFile.read(data);
		buffer.put(data);
		return buffer;
	}

	public ByteBuffer getDataByIndexNoSeek(long index) throws IOException {
		long offset = getOffsetByIndex(index);
		int length = (int) (getOffsetByIndex(index + 1) - offset);
		return readNoSeek(offset, length);
	}

	public HashMap<Integer, ByteBuffer> getRange(long index, int fetchNum) {
		HashMap<Integer, ByteBuffer> result = new HashMap<Integer, ByteBuffer>();
		try {
			dataFile.seek(getOffsetByIndex(index));
			for (int i = 0; i < fetchNum; i++) {
				result.put(i, getDataByIndexNoSeek(i + index));
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return result;
	}
}
