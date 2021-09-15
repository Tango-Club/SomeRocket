package io.openmessaging;

import java.io.File;
import java.nio.ByteBuffer;

public class Common {
	public static ByteBuffer cloneByteBuffer(final ByteBuffer original) {
		// Create clone with same capacity as original.
		final ByteBuffer clone = (original.isDirect()) ? ByteBuffer.allocateDirect(original.capacity())
				: ByteBuffer.allocate(original.capacity());

		// Create a read-only copy of the original.
		// This allows reading from the original without modifying it.
		final ByteBuffer readOnlyCopy = original.asReadOnlyBuffer();

		// Flip and read from the original.
		readOnlyCopy.flip();
		clone.put(readOnlyCopy);
		clone.position(original.position());
		clone.limit(original.limit());
		clone.order(original.order());
		return clone;
	}

	public static void cleanStorage() {
		File file = new File("/essd");
		if (!file.exists())
			return;
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
