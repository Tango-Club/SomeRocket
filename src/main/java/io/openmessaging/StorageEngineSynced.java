package io.openmessaging;

import org.apache.log4j.Logger;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class StorageEngineSynced {
	private static final Logger logger = Logger.getLogger(StorageEngineSynced.class);

	final String dataPath;
	RandomAccessFile dataFile; // length(short,2)|qid(short,2)|topicCode(byte,1)|data(length)
	FileChannel dataFileChannel;
	long dataNumber = 0;

	StorageEngineSynced(String storagePath) throws IOException {
		dataPath = storagePath + "/sync.data";
		Common.initPath(dataPath);

		try {
			dataFile = new RandomAccessFile(dataPath, "rw");
			dataFileChannel = dataFile.getChannel();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}

	public void flush() {
		try {
			dataFileChannel.force(false);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void write(Byte topicCode, short queueId, ByteBuffer buffer) throws IOException {
		ByteBuffer metaBuffer = ByteBuffer.allocateDirect(5);
		short length = (short) buffer.remaining();
		metaBuffer.putShort(length);
		metaBuffer.putShort(queueId);
		metaBuffer.put(topicCode);
		metaBuffer.flip();
		dataFileChannel.write(metaBuffer);
		dataFileChannel.write(buffer);
		dataNumber++;
	}
}
