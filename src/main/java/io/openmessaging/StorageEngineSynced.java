package io.openmessaging;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.io.RandomAccessFile;
import java.io.SyncFailedException;
import java.nio.channels.FileChannel;
import java.io.FileNotFoundException;
import org.apache.log4j.Logger;

public class StorageEngineSynced {
	String dataPath;
	String offsetPath;
	String qidPath;

	RandomAccessFile dataFile;
	RandomAccessFile offsetFile;
	RandomAccessFile qidFile;

	FileChannel dataFileChannel;

	private static Logger logger = Logger.getLogger(StorageEngine.class);

	long dataNumber;
	long lastOffset;

	private long getOffsetByIndex(long x) throws IOException {
		try {
			offsetFile.seek(x * 8);
			return offsetFile.readLong();
		} catch (IOException e) {
			e.printStackTrace();
			return -1;
		}
	}

	private void appendOffset(long offset) {
		try {
			offsetFile.seek(dataNumber * 8);
			offsetFile.writeLong(offset);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public int getQidByIndex(long x) {
		try {
			qidFile.seek(x * 4);
			return qidFile.readInt();
		} catch (IOException e) {
			e.printStackTrace();
			return -1;
		}
	}

	private void appendQid(int qid) {
		try {
			qidFile.seek(dataNumber * 4);
			qidFile.writeInt(qid);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void flush() {
		try {
			dataFileChannel.force(false);
			offsetFile.getFD().sync();
			qidFile.getFD().sync();
		} catch (SyncFailedException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	StorageEngineSynced(String storagePath, boolean exist) throws IOException {
		dataPath = storagePath + "/0.data";
		offsetPath = storagePath + "/0.offset";
		qidPath = storagePath + "/0.qid";

		Common.initPath(dataPath);
		Common.initPath(offsetPath);
		Common.initPath(qidPath);

		try {
			dataFile = new RandomAccessFile(dataPath, "rw");
			offsetFile = new RandomAccessFile(offsetPath, "rw");
			qidFile = new RandomAccessFile(qidPath, "rw");

			dataFileChannel = dataFile.getChannel();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}

		if (exist) {
			dataNumber = offsetFile.length() / 8 - 1;
			lastOffset = getOffsetByIndex(dataNumber - 1);
		} else {
			dataNumber = 0;
			lastOffset = 0;
			appendOffset(0);
		}
	}

	public void write(int queueId, ByteBuffer buffer) throws IOException {
		dataFileChannel.position(lastOffset);
		lastOffset += buffer.capacity();

		dataNumber++;
		dataFileChannel.write(buffer);
		appendOffset(lastOffset);
		appendQid(queueId);
	}

	public ByteBuffer readNoSeek(int length) throws IOException {
		ByteBuffer buffer = ByteBuffer.allocate(length);
		dataFileChannel.read(buffer);
		buffer.flip();
		return buffer;
	}

	public ByteBuffer getDataByIndex(long index) {
		try {
			long offset = getOffsetByIndex(index);
			dataFileChannel.position(offset);
			int length = (int) (getOffsetByIndex(index + 1) - offset);
			return readNoSeek(length);
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
	}

	public HashMap<Integer, ByteBuffer> getRange(int queueId, long index, long fetchNum) {
		HashMap<Integer, ByteBuffer> result = new HashMap<Integer, ByteBuffer>();
		int idx = 0;
		for (long i = 0; i < dataNumber && idx < fetchNum; i++) {
			if (getQidByIndex(i) != queueId)
				continue;
			if (index > 0) {
				index--;
				continue;
			}
			result.put(idx++, getDataByIndex(i));
		}
		return result;
	}
}
