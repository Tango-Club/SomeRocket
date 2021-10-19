package io.openmessaging;

import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;

public class StoragePageEssd {
	public int dataNumber;
	public int lastOffset;

	final ArrayList<Integer> offsets = new ArrayList<>();

	final String dataPath;

	RandomAccessFile dataFile;

	FileChannel dataFileChannel;

	boolean isReload;

	private static final Logger logger = Logger.getLogger(StorageEngineEssd.class);

	public void open() {
		try {
			dataFile = new RandomAccessFile(dataPath, "rw");
			dataFileChannel = dataFile.getChannel();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}

	private int getOffsetByIndex(int x) throws IOException {
		return offsets.get(x);
	}

	private void appendOffset(int offset) throws IOException {
		offsets.add(offset);
	}

	public boolean isReload() {
		return isReload;
	}

	StoragePageEssd(String baseStoragePath, boolean exist) throws IOException {
		dataPath = baseStoragePath + ".data";
		Common.initPath(dataPath);

		this.isReload = exist;

		open();

		this.dataNumber = 0;
		appendOffset(0);
	}

	public void delete() {
		File data = new File(dataPath);
		data.delete();
		offsets.clear();
	}

	public void write(ByteBuffer buffer) throws IOException {
		dataFileChannel.position(lastOffset);
		lastOffset += buffer.remaining();

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

	public ByteBuffer getDataByIndex(int index) throws IOException {
		int offset = getOffsetByIndex(index);
		dataFileChannel.position(offset);
		int length = getOffsetByIndex(index + 1) - offset;
		return readNoSeek(length);
	}

	public HashMap<Integer, ByteBuffer> getRange(int index, int fetchNum, int preFix) {
		HashMap<Integer, ByteBuffer> result = new HashMap<>();
		try {
			int preLength = getOffsetByIndex(index);
			dataFileChannel.position(preLength);
			for (int i = 0; i < fetchNum; i++) {
				int length = getOffsetByIndex(index+i+1);
				result.put(preFix + i, readNoSeek(length - preLength));
				preLength = length;
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return result;
	}
}
