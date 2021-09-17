package io.openmessaging;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;

public abstract class Common {
	final static int pageSize = 256 * 1024;

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

	private static void deleteDir(File file) {
		File[] contents = file.listFiles();
		if (contents == null) {
			file.delete();
			return;
		}

		for (File f : contents) {
			if (!Files.isSymbolicLink(f.toPath())) {
				deleteDir(f);
			}
		}

		file.delete();
	}

	public static void cleanPath(String path) {
		File file = new File(path);
		deleteDir(file);
	}

	public static boolean initDirectory(String basePath) {
		File file = new File(basePath);
		if (!file.exists()) {
			file.mkdir();
			return true;
		}
		return false;
	}

	public static boolean initPath(String path) throws IOException {
		File file = new File(path);
		if (!file.exists()) {
			file.createNewFile();
			return true;
		}
		return false;
	}

	public static ByteBuffer getByteBuffer(String str) {
		return ByteBuffer.wrap(str.getBytes());
	}

	public static String getString(ByteBuffer buffer) throws Exception {
		return new String(buffer.array(), 0, buffer.capacity(), "utf-8");
	}
}
