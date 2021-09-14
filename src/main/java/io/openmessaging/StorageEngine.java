package io.openmessaging;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.HashMap;
import org.apache.log4j.Logger;

public class StorageEngine {
	String dataPath;
	String offsetPath;

	RandomAccessFile dataFile;
	RandomAccessFile offsetFile;
	long dataNumber;
	long lastOffset;

	private static Logger logger = Logger.getLogger(StorageEngine.class);

	private void flush() throws IOException{
		dataFile.close();
		offsetFile.close();
		dataFile = new RandomAccessFile(dataPath, "rw");
		offsetFile = new RandomAccessFile(offsetPath, "rw");
	}

	private long getOffsetByIndex(long x) throws IOException {
		offsetFile.seek(x * 8);
		return offsetFile.readLong();
	}

	private void appendOffset(long offset) throws IOException {
		offsetFile.seek(offsetFile.length());
		offsetFile.writeLong(offset);
	}

	StorageEngine(String dataPath, String offsetPath, boolean exist) throws IOException {
		this.dataPath=dataPath;
		this.offsetPath=offsetPath;

		dataFile = new RandomAccessFile(dataPath, "rw");
		offsetFile = new RandomAccessFile(offsetPath, "rw");

		if (exist) {
			logger.info("reload: " + dataPath + ", " + offsetPath);
			dataNumber = (long) (offsetFile.length() / 8) - 1;
			lastOffset = getOffsetByIndex(dataNumber);
		} else {
			dataNumber = 0;
			lastOffset = 0;
			appendOffset(0);
		}
		dataFile.seek(lastOffset);
	}

	public synchronized long write(ByteBuffer buffer) throws IOException {
		dataFile.seek(dataFile.length());
		lastOffset += buffer.capacity();
		byte[] data = new byte[buffer.capacity()];

		buffer.get(data);
		dataFile.write(data);

		appendOffset(lastOffset);

		flush();
		dataNumber++;
		return dataNumber - 1;
	}

	public ByteBuffer readNoSeek(int length) throws IOException {
		ByteBuffer buffer = ByteBuffer.allocate(length);
		byte[] data = new byte[length];
		dataFile.read(data);
		buffer.put(data);
		buffer.flip();
		return buffer;
	}

	public ByteBuffer getDataByIndexNoSeek(long index) throws IOException {
		long offset = getOffsetByIndex(index);
		int length = (int) (getOffsetByIndex(index + 1) - offset);
		return readNoSeek(length);
	}

	public ByteBuffer getDataByIndex(long index) throws IOException {
		long offset = getOffsetByIndex(index);
		dataFile.seek(offset);
		int length = (int) (getOffsetByIndex(index + 1) - offset);
		return readNoSeek(length);
	}

	public synchronized HashMap<Integer, ByteBuffer> getRange(long index, int fetchNum) {
		fetchNum = (int) Math.min((long) fetchNum, dataNumber - index);
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
