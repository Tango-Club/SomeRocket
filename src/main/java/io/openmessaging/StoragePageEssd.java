package io.openmessaging;

import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;

public class StoragePageEssd {
	public int dataNumber;
	public int lastOffset;
	
	final String dataPath;
	final String offsetPath;

	RandomAccessFile dataFile;
	RandomAccessFile offsetFile;

	FileChannel dataFileChannel;

	boolean isReload;

	private static final Logger logger = Logger.getLogger(StorageEngineEssd.class);

	public void flush() {
		try {
			dataFileChannel.force(false);
			offsetFile.getFD().sync();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

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

	private int getOffsetByIndex(int x) throws IOException {
		offsetFile.seek((long) x << 2);
		return offsetFile.readInt();
	}

	private void appendOffset(int offset) throws IOException {
		offsetFile.seek((long) dataNumber << 2);
		offsetFile.writeInt(offset);
	}

	public boolean isReload() {
		return isReload;
	}

	StoragePageEssd(String basePath, boolean exist) throws IOException {
		this.dataPath = basePath + ".data";
		this.offsetPath = basePath + ".offset";
		Common.initPath(dataPath);
		Common.initPath(offsetPath);

		this.isReload = exist;

		open();

		if (exist) {
			this.dataNumber = ((int) offsetFile.length() >> 2) - 1;
			this.lastOffset = getOffsetByIndex(dataNumber - 1);
		} else {
			this.dataNumber = 0;
			this.lastOffset = 0;
			appendOffset(0);
		}
	}

	public void delete() {
		File data = new File(dataPath);
		data.delete();
		File offset = new File(offsetPath);
		offset.delete();
	}

	public void write(ByteBuffer buffer) throws IOException {
		dataFileChannel.position(lastOffset);
		lastOffset += buffer.capacity();

		dataNumber++;
		dataFileChannel.write(buffer);
		appendOffset(lastOffset);

	}

	private ByteBuffer readNoSeek(int length) throws IOException {
		ByteBuffer buffer = ByteBuffer.allocate(length);
		dataFileChannel.read(buffer);
		buffer.flip();
		return buffer;
	}

	private ByteBuffer getDataByIndexNoSeek(int index) throws IOException {
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
		HashMap<Integer, ByteBuffer> result = new HashMap<>();
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
