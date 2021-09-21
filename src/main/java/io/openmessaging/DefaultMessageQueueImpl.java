package io.openmessaging;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.*;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.Semaphore;

import org.apache.log4j.Logger;

public class DefaultMessageQueueImpl extends MessageQueue {
	private static Logger logger = Logger.getLogger(StorageEngine.class);
	ConcurrentHashMap<String, MessageBuffer> topicQueueMap = new ConcurrentHashMap<>();
	StorageEngineSynced backup;
	ConcurrentHashMap<String, Byte> topicCodeMap = new ConcurrentHashMap<>();

	StoragePage topicCodeDictPage;


	LinkedBlockingDeque<ArrayList<Object> > writeQueue = new LinkedBlockingDeque<ArrayList<Object> >();



	boolean isInited = false;


	Thread writeThread = new Thread(new Runnable() {
		@Override
		public void run() {
			Timer a = new Timer(true);
			a.schedule(new java.util.TimerTask() { public void run() { backup.flush();} }, 0, Common.syncTime);
			while (true) {
				try {
					ArrayList<Object> databox = writeQueue.poll();
					String topic=(String) databox.get(0);
					int queueId=(Integer) databox.get(1);
					ByteBuffer data = (ByteBuffer) databox.get(2);
					if (!isInited) {
						init();
						isInited = true;
					}
					Byte topicCode = endodeTopic(topic);
					try {
						backup.write(topicCode, (short) queueId, data);
					} catch (IOException e) {
						e.printStackTrace();
					}
					((Semaphore)databox.get(3)).release();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	});



	void init() {

		String runDir="";
		try{
			runDir=System.getenv("runDir");
		}catch (Exception e){
			e.printStackTrace();
		}
		Common.initDirectory(runDir+"/essd");
		Common.initDirectory(runDir+"/pmem");
		Common.initDirectory(runDir+"/essd/cache");
		Common.initDirectory(runDir+"/pmem/cache");

		String storagePath = runDir+"/essd/sync";
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
		String runDir="";
		try{
			runDir=System.getenv("runDir");
		}catch (Exception e){
			e.printStackTrace();
		}
		Common.cleanPath(runDir+"/essd/cache");
		Common.cleanPath(runDir+"/pmem/cache");

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
		try {
			ArrayList<Object> databox = new ArrayList<Object>();
			databox.add(topic);
			databox.add(queueId);
			databox.add(data);
			Semaphore semaphore = new Semaphore(0);
			databox.add(semaphore);
			writeQueue.add(databox);
			semaphore.acquire();

		}catch (Exception e){
			e.printStackTrace();
		}
//		synchronized (this) {
//			if (!isInited) {
//				init();
//				isInited = true;
//			}
//		}
//		Byte topicCode = endodeTopic(topic);
//		try {
//			backup.write(topicCode, (short) queueId, data);
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
//		try {
//			synchronized (this) {
//				long tBefore = System.nanoTime();
//				wait(0, Common.syncTime);
//				if (System.nanoTime() - tBefore >= Common.syncTime) {
//					backup.flush();
//					notifyAll();
//				}
//			}
//		} catch (InterruptedException e) {
//			e.printStackTrace();
//		}

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
