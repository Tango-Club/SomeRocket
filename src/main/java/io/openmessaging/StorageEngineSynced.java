package io.openmessaging;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.io.RandomAccessFile;
import java.io.SyncFailedException;
import java.nio.channels.FileChannel;
import java.io.FileDescriptor;
import java.io.FileNotFoundException;
import org.apache.log4j.Logger;

public class StorageEngineSynced {
	String dataPath;
	String metaPath;

	RandomAccessFile dataFile;
	RandomAccessFile metaFile;// offset(long,8)|qid(short,2)|topicCode(byte,1)

	FileChannel dataFileChannel;

	private static Logger logger = Logger.getLogger(StorageEngine.class);

	long dataNumber;
	long lastOffset;
	FileDescriptor metaFileFD;

	private void appendMeta(long offset, short queueId, byte topicCode) {
		try {
			metaFile.seek(dataNumber * 11);
			metaFile.writeLong(offset);
			metaFile.writeShort(queueId);
			metaFile.writeByte(topicCode);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void flush() {
		try {
			dataFileChannel.force(false);
			metaFileFD.sync();
		} catch (SyncFailedException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	StorageEngineSynced(String storagePath, boolean exist) throws IOException {
		dataPath = storagePath + "/sync.data";
		metaPath = storagePath + "/sync.meta";


		Common.initPath(dataPath);
		Common.initPath(metaPath);

		try {
			dataFile = new RandomAccessFile(dataPath, "rw");
			metaFile = new RandomAccessFile(metaPath, "rw");

			dataFileChannel = dataFile.getChannel();
			metaFileFD = metaFile.getFD();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}

		if (exist) {
			dataNumber = metaFile.length() / 11 - 1;
			metaFile.seek((dataNumber - 1) * 11);
			lastOffset = metaFile.readLong();
		} else {
			dataNumber = 0;
			lastOffset = 0;
			appendMeta((long) 0, (short) 0, (byte) 0);
		}
	}

	public void write(Byte topicCode, short queueId, ByteBuffer buffer) throws IOException {
		dataFileChannel.position(lastOffset);
		lastOffset += buffer.capacity();

		dataNumber++;
		dataFileChannel.write(buffer);
		appendMeta(lastOffset, queueId, topicCode);
	}

	public ByteBuffer readNoSeek(int length) throws IOException {
		ByteBuffer buffer = ByteBuffer.allocate(length);
		dataFileChannel.read(buffer);
		buffer.flip();
		return buffer;
	}

	public ByteBuffer getDataByIndex(long index) {
		try {
			metaFile.seek(index * 11);
			long offset = metaFile.readLong();
			metaFile.seek(index * 11 + 11);
			dataFileChannel.position(offset);
			return readNoSeek((int) (metaFile.readLong() - offset));
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
	}
}
