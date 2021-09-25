package io.openmessaging;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.prefs.BackingStoreException;
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
			} catch (IOException | InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	private void recover() throws IOException, InterruptedException {
		Common.cleanPath(Common.runDir + "/essd/cache");
		Common.cleanPath(Common.runDir + "/pmem/cache");

		ConcurrentHashMap<Byte, String> reverseMap = new ConcurrentHashMap<>();
		for (byte i = 0; i < topicCodeDictPage.dataNumber; i++) {
			String topic;
			topic = Common.getString(topicCodeDictPage.getDataByIndex(i));
			tryCreatStorage(topic);
			reverseMap.put((Byte) i, topic);
			topicCodeMap.put(topic, (Byte) i);
		}

		long fileLength = backup.dataFile.length();
		for (long i = 0; i < fileLength;) {
			short length = backup.dataFile.readShort();
			short queueId = backup.dataFile.readShort();
			Byte topicCode = backup.dataFile.readByte();
			ByteBuffer buffer = ByteBuffer.allocate(length);
			byte[] data = new byte[length];
			backup.dataFile.read(data);
			buffer.put(data);
			buffer.flip();
			i += 5 + length;
			backup.dataNumber++;

			String topic = reverseMap.get(topicCode);
			topicQueueMap.get(topic).appendData(queueId, buffer);
		}
	}

	private void ready(String topic) {
		tryInit();
		tryCreatStorage(topic);
	}

	private void tryInit() {
		if (!isInited) {
			synchronized (this) {
				if (!isInited) {
					init();
					isInited = true;
				}
			}
		}
	}

	private void tryCreatStorage(String topic) {
		if (topicQueueMap.containsKey(topic))
			return;
		synchronized (this) {
			if (!topicQueueMap.containsKey(topic)) {
				try {
					topicQueueMap.put(topic, new MessageBuffer(topic));
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	@Override
	public long append(String topic, int queueId, ByteBuffer data) {
		ready(topic);
		Byte topicCode = endodeTopic(topic);
		long now = backup.dataNumber + 1;
		try {
			backup.write(topicCode, (short) queueId, data);
		} catch (IOException e) {
			e.printStackTrace();
		}
		data.position(0);
		long result = -1;
		try {
			result = topicQueueMap.get(topic).appendData(queueId, data);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		if (lastFlush < now && backup.dataNumber == now) {
			try {
				TimeUnit.MICROSECONDS.sleep(500);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			if (lastFlush < now) {
				lastFlush = backup.dataNumber;
				backup.flush();
			}
		}
		return result;
	}

	private Byte endodeTopic(String topic) {
		if (topicCodeMap.containsKey(topic)) {
			return topicCodeMap.get(topic);
		}
		synchronized (this) {
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
		}
		return topicCodeMap.get(topic);
	}

	@Override
	public Map<Integer, ByteBuffer> getRange(String topic, int queueId, long offset, int fetchNum) {
		ready(topic);
		return topicQueueMap.get(topic).getRange(queueId, offset, fetchNum);
	}
}
