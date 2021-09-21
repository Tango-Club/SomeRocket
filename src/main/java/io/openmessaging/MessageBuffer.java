package io.openmessaging;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.log4j.Logger;

public class MessageBuffer {
	ConcurrentHashMap<Integer, DiskStorage> cacheMap = new ConcurrentHashMap<>();
	String topic;
	String cachePath;

	private static Logger logger = Logger.getLogger(MessageBuffer.class);

	private synchronized void creatStorage(int queueId) {
		if (!cacheMap.containsKey(queueId)) {
			try {
				String runDir="";
				try{
					runDir=System.getenv("runDir");
				}catch (Exception e){
					e.printStackTrace();
				}
				String cachePath;
				if (queueId % 3 != 0)
					cachePath = runDir+"/essd/cache";
				else
					cachePath = runDir+"/pmem/cache";

				cacheMap.put(queueId, new DiskStorage(topic, queueId, cachePath));
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	MessageBuffer(String topic) throws IOException {
		this.topic = topic;
	}

	public long appendData(int queueId, ByteBuffer data) throws IOException, InterruptedException {
		creatStorage(queueId);
		return cacheMap.get(queueId).writeToDisk(data);
	}

	public HashMap<Integer, ByteBuffer> getRange(int queueId, long offset, int fetchNum) {
		creatStorage(queueId);
		return cacheMap.get(queueId).readFromDisk(offset, fetchNum);
	}
}
