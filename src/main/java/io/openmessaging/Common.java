package io.openmessaging;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import com.intel.pmem.llpl.Heap;

public abstract class Common {
	final static int pageSize = 64 * 1024;
	final static long heapSize = 59l * 1024 * 1024 * 1024;
	public static Heap heap;
	public static String runDir;

	public static String readEnvInfo() throws IOException {
		final ProcessBuilder pb = new ProcessBuilder("sh", "-c", "getconf -a");
		pb.redirectErrorStream(true);

		final Process process = pb.start();
		final InputStream in = process.getInputStream();
		BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
		StringBuilder result = new StringBuilder();
		String line;
		while ((line = bufferedReader.readLine()) != null) {
			result.append(line);
			result.append("\n");
		}
		return result.toString();

	}

	public static ByteBuffer clone(ByteBuffer original) {
		ByteBuffer clone = ByteBuffer.allocate(original.remaining());
		clone.put(original);
		clone.flip();
		return clone;
	}

	private static void deleteDir(File file) {
		File[] contents = file.listFiles();
		if (contents == null) {
			file.delete();
			return;
		}

		for (File f : contents) {
			deleteDir(f);
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

	public static void initPath(String path) throws IOException {
		File file = new File(path);
		if (!file.exists()) {
			file.createNewFile();
		}
	}

	public static ByteBuffer getByteBuffer(String str) {
		return ByteBuffer.wrap(str.getBytes());
	}

	public static String getString(ByteBuffer buffer) {
		return new String(buffer.array(), 0, buffer.capacity(), StandardCharsets.UTF_8);
	}
}
