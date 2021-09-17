package io.openmessaging;

import java.nio.ByteBuffer;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

public class WriteBenchmarkTest {
	MessageQueue messageQueue = new DefaultMessageQueueImpl();
	// MessageQueue messageQueue=new SampleMessageQueueImpl();

	@Test
	public void main0() {

		Common.cleanPath("/essd/");
		Common.cleanPath("/pmem/");

		String text = "Hello Message Queue!";
		String topic = "TestTopic";
		int queueId = 123;

		try {
			int x = 1000;
			for (int i = 0; i < x; i++)
				messageQueue.append(topic, queueId, Common.getByteBuffer(text));
			
			Map<Integer, ByteBuffer> resultMap = messageQueue.getRange(topic, queueId, 0, x);
			
			Assert.assertEquals(x, resultMap.size());
			for (int i = 0; i < x; i++) {
				String msgRead = Common.getString(resultMap.get(i));
				Assert.assertEquals(msgRead, text);
			}
			
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail();
		}

	} // End of main function
}
