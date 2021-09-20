package io.openmessaging;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.*;
import org.apache.log4j.Logger;

public class DefaultMessageQueueImpl extends MessageQueue {
	private static Logger logger = Logger.getLogger(StorageEngine.class);
	ConcurrentHashMap<String, MessageBuffer> topicQueueMap = new ConcurrentHashMap<>();
	StorageEngineSynced backup;
	ConcurrentHashMap<String, Byte> topicCodeMap = new ConcurrentHashMap<>();

	StoragePage topicCodeDictPage;

	boolean isInited = false;

	void init() {
		Common.initDirectory("/essd");
		Common.initDirectory("/pmem");
		Common.initDirectory("/essd/cache");
		Common.initDirectory("/pmem/cache");

		String storagePath = "/essd/sync";
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
		Common.cleanPath("/essd/cache");
		Common.cleanPath("/pmem/cache");

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
		if (!isInited) {
			isInited = true;
			init();
		}
		Byte topicCode = endodeTopic(topic);
		try {
			backup.write(topicCode, (short) queueId, data);
		} catch (IOException e) {
			e.printStackTrace();
		}
		try {
			synchronized (this) {
				long tBefore = System.currentTimeMillis();
				wait(Common.syncTime);
				if (System.currentTimeMillis() - tBefore >= Common.syncTime) {
					backup.flush();
					notifyAll();
				}
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
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
		if (!isInited) {
			isInited = true;
			init();
		}
		creatStorage(topic);
		return topicQueueMap.get(topic).getRange(queueId, offset, fetchNum);
	}
}
