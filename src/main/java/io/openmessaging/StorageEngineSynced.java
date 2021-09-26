package io.openmessaging;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.io.RandomAccessFile;
import java.io.FileNotFoundException;

public class StorageEngineSynced {
	final String dataPath;
	RandomAccessFile dataFile; // length(short,2)|qid(short,2)|topicCode(byte,1)|data(length)
	long dataNumber = 0;

	public void flush() {
		try {
			dataFile.getFD().sync();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	StorageEngineSynced(String storagePath) throws IOException {
		dataPath = storagePath + "/sync.data";
		Common.initPath(dataPath);

		try {
			dataFile = new RandomAccessFile(dataPath, "rw");
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}

	public void write(Byte topicCode, short queueId, ByteBuffer buffer) throws IOException {
		short length = (short) buffer.capacity();
		dataFile.writeShort(length);
		dataFile.writeShort(queueId);
		dataFile.writeByte(topicCode);
		byte[] data = new byte[length];
		buffer.get(data);
		dataFile.write(data);
		dataNumber++;
	}
}
