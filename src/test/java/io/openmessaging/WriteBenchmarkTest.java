package io.openmessaging;

import java.io.FileNotFoundException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.junit.Assert;
import org.junit.Test;

public class WriteBenchmarkTest {

	@Test
	public void main0() {
		Common.cleanPath(Common.runDir + "/essd/");
		Common.cleanPath(Common.runDir + "/pmem/");

		MessageQueue messageQueue = new DefaultMessageQueueImpl();

		String text = "Hello Message Queue!";
		String topic = "TestTopic";
		int queueId = 123;

		try {
			int x = 1000;
			for (int i = 0; i < x; i++) {
				CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
					try {
						messageQueue.append(topic, queueId, Common.getByteBuffer(text));
					} catch (FileNotFoundException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				});
				CompletableFuture<Void> future2 = CompletableFuture.runAsync(() -> {
					try {
						messageQueue.append(topic, queueId + 1, Common.getByteBuffer(text));
					} catch (FileNotFoundException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				});
				CompletableFuture<Void> future3 = CompletableFuture.runAsync(() -> {
					try {
						messageQueue.append(topic, queueId + 2, Common.getByteBuffer(text));
					} catch (FileNotFoundException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				});
				CompletableFuture<Void> future4 = CompletableFuture.runAsync(() -> {
					try {
						messageQueue.append(topic, queueId + 3, Common.getByteBuffer(text));
					} catch (FileNotFoundException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				});
				CompletableFuture<Void> future5 = CompletableFuture.runAsync(() -> {
					try {
						messageQueue.append(topic, queueId + 4, Common.getByteBuffer(text));
					} catch (FileNotFoundException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				});
				future.get();
				future2.get();
				future3.get();
				future4.get();
				future5.get();
			}

			for (int i = 0; i < x; i++) {
				Map<Integer, ByteBuffer> resultMap = messageQueue.getRange(topic, queueId, i, 1);
				Assert.assertEquals(1, resultMap.size());
				String msgRead = Common.getString(resultMap.get(0));
				Assert.assertEquals(msgRead, text);
			}

		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail();
		}

	} // End of main function
}
