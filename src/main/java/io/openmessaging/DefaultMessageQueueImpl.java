package io.openmessaging;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.*;
import org.apache.log4j.Logger;

public class DefaultMessageQueueImpl extends MessageQueue {
	private static Logger logger = Logger.getLogger(DefaultMessageQueueImpl.class);

	ConcurrentHashMap<String, MessageBuffer> topicQueueMap = new ConcurrentHashMap<>();
	StorageEngineSynced backup;
	ConcurrentHashMap<String, Byte> topicCodeMap = new ConcurrentHashMap<>();

	StoragePage topicCodeDictPage;

	boolean isInited = false;
	long lastFlush = -1;

	void init() {
		try {
			logger.info(Common.readCpuCache());
		} catch (Exception e) {
		}

		if (Common.runDir == null)
			Common.runDir = "";
		Common.initDirectory(Common.runDir + "/essd");
		Common.initDirectory(Common.runDir + "/pmem");
		Common.initDirectory(Common.runDir + "/essd/cache");
		Common.initDirectory(Common.runDir + "/pmem/cache");

		String storagePath = Common.runDir + "/essd/sync";

		boolean isReload = !Common.initDirectory(storagePath);
		try {
			backup = new StorageEngineSynced(storagePath, isReload);
		} catch (IOException e) {
			e.printStackTrace();
		}

		String dictPath = storagePath + "/dict";
		try {
			topicCodeDictPage = new StoragePage(dictPath, isReload);
		} catch (IOException e) {
			e.printStackTrace();
		}

		if (isReload) {
			try {
				recover();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	private void recover() throws IOException {
		Common.cleanPath(Common.runDir + "/essd/cache");
		Common.cleanPath(Common.runDir + "/pmem/cache");

		ConcurrentHashMap<Byte, String> reverseMap = new ConcurrentHashMap<>();
		for (byte i = 0; i < topicCodeDictPage.dataNumber; i++) {

			String topic;
			try {
				topic = Common.getString(topicCodeDictPage.getDataByIndex(i));
				creatStorage(topic);
				reverseMap.put((Byte) i, topic);
				topicCodeMap.put(topic, (Byte) i);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		long lastOffset = 0;
		backup.dataFileChannel.position(0);
		for (long i = 1; i <= backup.dataNumber; i++) {
			backup.metaFile.seek(11 * i);
			long offset = backup.metaFile.readLong();
			int queueId = backup.metaFile.readShort();
			Byte topicCode = backup.metaFile.readByte();
			ByteBuffer buffer = backup.readNoSeek((int) (offset - lastOffset));

			lastOffset = offset;

			String topic = reverseMap.get(topicCode);
			try {
				topicQueueMap.get(topic).appendData(queueId, buffer);
			} catch (IOException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	private synchronized void creatStorage(String topic) {
		if (!topicQueueMap.containsKey(topic)) {
			try {
				topicQueueMap.put(topic, new MessageBuffer(topic));
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	@Override
	public long append(String topic, int queueId, ByteBuffer data) {
		synchronized (this) {
			if (!isInited) {
				init();
				isInited = true;
			}
		}
		Byte topicCode = endodeTopic(topic);
		try {
			backup.write(topicCode, (short) queueId, data);
		} catch (IOException e) {
			e.printStackTrace();
		}

		try {
			synchronized (this) {
				long nowNum = backup.dataNumber;
				wait(0, Common.syncTime);
				synchronized (this) {
					if (lastFlush < backup.dataNumber) {
						backup.flush();
						lastFlush = backup.dataNumber;
						notifyAll();
					}
				}
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		if (backup.dataNumber % 1000000 == 0)
			try {
				logger.info("dataNum: " + backup.dataNumber + " fileSize: " + backup.dataFile.length());
			} catch (IOException e1) {
				e1.printStackTrace();
			}
		data.position(0);
		creatStorage(topic);
		try {
			return topicQueueMap.get(topic).appendData(queueId, data);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return -1;
	}

	private Byte endodeTopic(String topic) {
		if (topicCodeMap.containsKey(topic)) {
			return topicCodeMap.get(topic);
		}
		int mapSize = topicCodeMap.size();
		topicCodeMap.put(topic, (byte) mapSize);
		try {
			topicCodeDictPage.write(Common.getByteBuffer(topic));
			topicCodeDictPage.flush();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return (byte) mapSize;
	}

	@Override
	public Map<Integer, ByteBuffer> getRange(String topic, int queueId, long offset, int fetchNum) {
		synchronized (this) {
			if (!isInited) {
				init();
				isInited = true;
			}
		}
		creatStorage(topic);
		return topicQueueMap.get(topic).getRange(queueId, offset, fetchNum);
	}
}
