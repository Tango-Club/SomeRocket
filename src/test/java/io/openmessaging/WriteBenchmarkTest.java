package io.openmessaging;

import java.io.FileNotFoundException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
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
			int x = 100;
			int y = 50;
			for (int i = 0; i < x; i++) {
				ArrayList<CompletableFuture<Void>> futures = new ArrayList<>();
				for (int j = 0; j < y; j++) {
					final int id = queueId + j;
					CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
						try {
							messageQueue.append(topic, id, Common.getByteBuffer(text));
						} catch (FileNotFoundException e) {
							e.printStackTrace();
						}
					});
					futures.add(future);
				}
				for (CompletableFuture<Void> future : futures)
					future.get();
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
