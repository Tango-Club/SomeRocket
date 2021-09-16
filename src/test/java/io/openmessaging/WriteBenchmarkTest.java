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

		String text = "Hello Message Queue!";
		String topic = "TestTopic";
		int queueId = 123;

		try {
			for (int i = 0; i < 1000; i++)
				messageQueue.append(topic, queueId, Common.getByteBuffer(text));

			Map<Integer, ByteBuffer> resultMap = messageQueue.getRange(topic, queueId, 0, 1);

			Assert.assertNotEquals(resultMap.size(), 0);

			String msgRead = Common.getString(resultMap.get(0));

			Assert.assertEquals(msgRead, text);
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail();
		}

	} // End of main function
}
