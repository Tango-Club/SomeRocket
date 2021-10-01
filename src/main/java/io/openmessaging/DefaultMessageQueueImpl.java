package io.openmessaging;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.intel.pmem.llpl.Heap;

public class DefaultMessageQueueImpl extends MessageQueue {
	private static final Logger logger = Logger.getLogger(DefaultMessageQueueImpl.class);

	final ConcurrentHashMap<String, MessageBuffer> topicQueueMap = new ConcurrentHashMap<>();
	final ConcurrentHashMap<String, Byte> topicCodeMap = new ConcurrentHashMap<>();
	StorageEngineSynced backup;
	StoragePageEssd topicCodeDictPage;

	boolean isInited = false;
	long lastFlush = -1;

	void init() {
		try {
			Common.runDir = System.getenv("runDir");
			// logger.info(Common.readEnvInfo());
		} catch (Exception e) {
			e.printStackTrace();
		}
		if (Common.runDir == null)
			Common.runDir = "";

		Common.initDirectory(Common.runDir + "/pmem");
		Common.initDirectory(Common.runDir + "/pmem/cache");

		String heapPath = Common.runDir + "/pmem/heap0";
		boolean initialized = Heap.exists(heapPath);
		logger.info("heap initialized: " + initialized);
		Common.heap = initialized ? Heap.openHeap(heapPath) : Heap.createHeap(heapPath, Common.heapSize);

		Common.initDirectory(Common.runDir + "/essd");
		Common.initDirectory(Common.runDir + "/essd/cache");

		String storagePath = Common.runDir + "/essd/sync";

		boolean isReload = !Common.initDirectory(storagePath);
		try {
			backup = new StorageEngineSynced(storagePath);
		} catch (IOException e) {
			e.printStackTrace();
		}

		String dictPath = storagePath + "/dict";
		try {
			topicCodeDictPage = new StoragePageEssd(dictPath, dictPath, isReload);
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
			topic = Common.getString(topicCodeDictPage.getDataByIndex(i));
			tryCreateStorage(topic);
			reverseMap.put(i, topic);
			topicCodeMap.put(topic, i);
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
		tryCreateStorage(topic);
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

	private void tryCreateStorage(String topic) {
		if (topicQueueMap.containsKey(topic))
			return;
		synchronized (this) {
			if (!topicQueueMap.containsKey(topic)) {
				topicQueueMap.put(topic, new MessageBuffer(topic));
			}
		}
	}

	@Override
	public long append(String topic, int queueId, ByteBuffer data) {
		ready(topic);
		Byte topicCode = encodeTopic(topic);
		long now = backup.dataNumber + 1;
		try {
			backup.write(topicCode, (short) queueId, data);
		} catch (IOException e) {
			e.printStackTrace();
		}
		data.flip();
		long result = -1;
		try {
			result = topicQueueMap.get(topic).appendData(queueId, data);
		} catch (IOException e) {
			e.printStackTrace();
		}
		if (lastFlush < now && backup.dataNumber == now) {
			if (lastFlush < now) {
				lastFlush = backup.dataNumber;
				backup.flush();
			}
		}
		return result;
	}

	private Byte encodeTopic(String topic) {
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
