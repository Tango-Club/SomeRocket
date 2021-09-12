package io.openmessaging;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

public class CorrectTest {
	MessageQueue messageQueue = new DefaultMessageQueueImpl();
	// MessageQueue messageQueue=new SampleMessageQueueImpl();

	public static ByteBuffer getByteBuffer(String str) {
		return ByteBuffer.wrap(str.getBytes());
	}

	public static String getString(ByteBuffer buffer) throws Exception {
		Charset charset = null;
		CharsetDecoder decoder = null;
		CharBuffer charBuffer = null;

		charset = Charset.forName("UTF-8");
		decoder = charset.newDecoder();
		// 用这个的话，只能输出来一次结果，第二次显示为空
		// charBuffer = decoder.decode(buffer);
		charBuffer = decoder.decode(buffer.asReadOnlyBuffer());
		return charBuffer.toString();
	}

	@Test
	public void main0() {
		String text = "Hello Message Queue!";
		String topic = "Test Topic";
		int queueId = 123;

		try {
			messageQueue.append(topic, queueId, getByteBuffer(text));

			Map<Integer, ByteBuffer> resultMap = messageQueue.getRange(topic, queueId, 0, 1);

			String msgRead = getString(resultMap.get(0));

			Assert.assertEquals(msgRead, text);
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail();
		}
	} // End of main function
}
