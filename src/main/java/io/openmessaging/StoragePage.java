package io.openmessaging;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import org.apache.log4j.Logger;

final public class StoragePage {
	String dataPath;
	String offsetPath;

	RandomAccessFile dataFile;
	RandomAccessFile offsetFile;

	FileChannel dataFileChannel;

	int dataNumber;
	int lastOffset;

	boolean isReload;

	private static Logger logger = Logger.getLogger(StorageEngine.class);

	public void open() {
		try {
			dataFile = new RandomAccessFile(dataPath, "rw");
			offsetFile = new RandomAccessFile(offsetPath, "rw");
			dataFileChannel = dataFile.getChannel();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}

	public void close() {
		try {
			dataFileChannel.close();
			dataFile.close();
			offsetFile.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void flush() throws IOException {
		Runtime.getRuntime().exec("sync");
		//dataFileChannel.force(false);
		//offsetFile.getFD().sync();
	}

	private int getOffsetByIndex(int x) throws IOException {
		offsetFile.seek(x * 4);
		return offsetFile.readInt();
	}

	private void appendOffset(int offset) throws IOException {
		offsetFile.seek(dataNumber * 4);
		offsetFile.writeInt(offset);
	}

	public boolean isReload() {
		return isReload;
	}

	StoragePage(String basePath, boolean exist) throws IOException {
		this.dataPath = basePath + ".data";
		this.offsetPath = basePath + ".offset";
		Common.initPath(dataPath);
		Common.initPath(offsetPath);

		this.isReload = exist;

		open();

		if (exist) {
			this.dataNumber = (int) offsetFile.length() / 4 - 1;
			this.lastOffset = getOffsetByIndex(dataNumber - 1);
			logger.info("reload: " + dataPath + ", " + offsetPath + ", dataNumber: " + dataNumber);
		} else {
			this.dataNumber = 0;
			this.lastOffset = 0;
			appendOffset(0);
		}
	}

	public void write(ByteBuffer buffer) throws IOException {
		dataFileChannel.position(lastOffset);
		lastOffset += buffer.capacity();

		dataNumber++;
		dataFileChannel.write(buffer);
		appendOffset(lastOffset);

	}

	public ByteBuffer readNoSeek(int length) throws IOException {
		ByteBuffer buffer = ByteBuffer.allocate(length);
		dataFileChannel.read(buffer);
		buffer.flip();
		return buffer;
	}

	public ByteBuffer getDataByIndexNoSeek(int index) throws IOException {
		int offset = getOffsetByIndex(index);
		int length = getOffsetByIndex(index + 1) - offset;
		return readNoSeek(length);
	}

	public ByteBuffer getDataByIndex(int index) throws IOException {
		int offset = getOffsetByIndex(index);
		dataFileChannel.position(offset);
		int length = getOffsetByIndex(index + 1) - offset;
		return readNoSeek(length);
	}

	public HashMap<Integer, ByteBuffer> getRange(int index, int fetchNum, int preFix) {
		HashMap<Integer, ByteBuffer> result = new HashMap<Integer, ByteBuffer>();
		try {
			dataFileChannel.position(getOffsetByIndex(index));
			for (int i = 0; i < fetchNum; i++) {
				result.put(preFix + i, getDataByIndexNoSeek(i + index));
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return result;
	}
}
