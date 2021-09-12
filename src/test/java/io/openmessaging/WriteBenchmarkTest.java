package io.openmessaging;

import java.nio.ByteBuffer;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

public class WriteBenchmarkTest {
	MessageQueue messageQueue = new DefaultMessageQueueImpl();
	// MessageQueue messageQueue=new SampleMessageQueueImpl();

	public static ByteBuffer getByteBuffer(String str) {
		return ByteBuffer.wrap(str.getBytes());
	}

	public static String getString(ByteBuffer buffer) throws Exception {
		return new String(buffer.array(), 0, buffer.capacity(), "utf-8");
	}

	@Test
	public void main0() {
		String text = "Hello Message Queue!";
		String topic = "TestTopic";
		int queueId = 123;

		try {
			for (int i = 0; i < 100000; i++)
				messageQueue.append(topic, queueId, getByteBuffer(text));

			Map<Integer, ByteBuffer> resultMap = messageQueue.getRange(topic, queueId, 0, 1);

			Assert.assertNotEquals(resultMap.size(), 0);

			String msgRead = getString(resultMap.get(0));

			Assert.assertEquals(msgRead, text);
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail();
		}
	} // End of main function
}
