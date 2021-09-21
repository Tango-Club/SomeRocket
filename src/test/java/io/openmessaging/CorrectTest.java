package io.openmessaging;

import java.nio.ByteBuffer;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

public class CorrectTest {

	@Test
	public void main0() {
		Common.cleanPath(Common.runDir + "/essd/");
		Common.cleanPath(Common.runDir + "/pmem/");

		MessageQueue messageQueue = new DefaultMessageQueueImpl();
		try {
			long res = -1;

			res = messageQueue.append("a", 1001, Common.getByteBuffer("2021"));
			Assert.assertEquals(0, res);

			res = messageQueue.append("b", 1001, Common.getByteBuffer("2022"));
			Assert.assertEquals(0, res);

			res = messageQueue.append("a", 1000, Common.getByteBuffer("2023"));
			Assert.assertEquals(0, res);

			res = messageQueue.append("b", 1001, Common.getByteBuffer("2024"));
			Assert.assertEquals(1, res);

			Map<Integer, ByteBuffer> mp;

			mp = messageQueue.getRange("a", 1000, 1, 2);
			Assert.assertEquals(0, mp.size());

			mp = messageQueue.getRange("b", 1001, 0, 2);
			Assert.assertEquals(2, mp.size());
			Assert.assertEquals("2022", Common.getString(mp.get(0)));
			Assert.assertEquals("2024", Common.getString(mp.get(1)));

			mp = messageQueue.getRange("b", 1001, 1, 2);
			Assert.assertEquals(1, mp.size());
			Assert.assertEquals("2024", Common.getString(mp.get(0)));

			messageQueue = new DefaultMessageQueueImpl(); // simulate restart

			mp = messageQueue.getRange("a", 1000, 1, 2);
			Assert.assertEquals(0, mp.size());

			mp = messageQueue.getRange("b", 1001, 0, 2);
			Assert.assertEquals(2, mp.size());
			Assert.assertEquals("2022", Common.getString(mp.get(0)));
			Assert.assertEquals("2024", Common.getString(mp.get(1)));

			mp = messageQueue.getRange("b", 1001, 1, 2);
			Assert.assertEquals(1, mp.size());
			Assert.assertEquals("2024", Common.getString(mp.get(0)));
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail();
		}
	}

	// End of main function
}
