package io.openmessaging;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;

import org.apache.log4j.Logger;

public class StorageEngine {
	String dataPath;
	String offsetPath;

	MappedByteBuffer  dataFile;
	MappedByteBuffer offsetFile;
	long dataNumber;
	long lastOffset;

	boolean isReload = false;
	boolean alwaysFlush;

	private static Logger logger = Logger.getLogger(StorageEngine.class);

	private void flush() throws IOException {
		if (!alwaysFlush)
			return;
		dataFile.force();
		offsetFile.force();
	}

	private long getOffsetByIndex(long x) throws IOException {
		offsetFile.position((int) (x*8));
		return offsetFile.getLong();
	}

	private void appendOffset(long offset) throws IOException {
		offsetFile.position(offsetFile.limit());
		offsetFile.putLong(offset);
	}

	public boolean isReload() {
		return isReload;
	}

	StorageEngine(String dataPath, String offsetPath, boolean exist, boolean alwaysFlush) throws IOException {
		this.alwaysFlush = alwaysFlush;
		this.dataPath = dataPath;
		this.offsetPath = offsetPath;

		dataFile = new RandomAccessFile(dataPath, "rw").getChannel().map(FileChannel.MapMode.READ_WRITE, 0, 64*1024);
		offsetFile = new RandomAccessFile(offsetPath, "rw").getChannel().map(FileChannel.MapMode.READ_WRITE, 0, 64*1024);

		if (exist) {
			isReload = true;
			dataNumber = (long) (offsetFile.limit() / 8) - 1;
			lastOffset = getOffsetByIndex(dataNumber);
			logger.info("reload: " + dataPath + ", " + offsetPath + ", dataNumber: " + dataNumber);
		} else {
			dataNumber = 0;
			lastOffset = 0;
			appendOffset(0);
		}
		dataFile.position((int) lastOffset);
	}

	public synchronized long write(ByteBuffer buffer) throws IOException {
		dataFile.position(dataFile.limit());
		lastOffset += buffer.capacity();

		dataFile.put(buffer);

		appendOffset(lastOffset);

		flush();
		dataNumber++;
		return dataNumber - 1;
	}

	public ByteBuffer readNoSeek(int length) throws IOException {
		ByteBuffer buffer = ByteBuffer.allocate(length);
		byte[] data = new byte[length];
		dataFile.get(data);
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
		dataFile.position((int) offset);
		int length = (int) (getOffsetByIndex(index + 1) - offset);
		return readNoSeek(length);
	}

	public synchronized HashMap<Integer, ByteBuffer> getRange(long index, int fetchNum) {
		fetchNum = (int) Math.min((long) fetchNum, dataNumber - index);
		HashMap<Integer, ByteBuffer> result = new HashMap<Integer, ByteBuffer>();
		try {
			if (fetchNum > 0)
				dataFile.position((int) getOffsetByIndex(index));
			for (int i = 0; i < fetchNum; i++) {
				result.put(i, getDataByIndexNoSeek(i + index));
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return result;
	}
}
