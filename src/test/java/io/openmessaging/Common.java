package io.openmessaging;

import java.io.File;
import java.nio.ByteBuffer;

public class Common {
	public static void cleanStorage() {
		File file = new File("./storage");
		String[] children = file.list();
		for (int i = 0; i < children.length; i++) {
			new File(file, children[i]).delete();
		}
	}

	public static ByteBuffer getByteBuffer(String str) {
		return ByteBuffer.wrap(str.getBytes());
	}

	public static String getString(ByteBuffer buffer) throws Exception {
		return new String(buffer.array(), 0, buffer.capacity(), "utf-8");
	}
}
