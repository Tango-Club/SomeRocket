package io.openmessaging;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.io.InputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.lang.Process;

public abstract class Common {
	final static int pageSize = 256 * 1024;
	final static int syncTimeNs = 0;
	final static int syncTimeMs = 1;

	static String runDir;

	public static final String readCpuCache() throws IOException {
		final ProcessBuilder pb = new ProcessBuilder("sh", "-c", "getconf -a |grep CACHE");
		pb.redirectErrorStream(true);

		final Process process = pb.start();
		final InputStream in = process.getInputStream();
		BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(in, "UTF-8"));
		String result = "";
		String line;
		while ((line = bufferedReader.readLine()) != null) {
			result += line;
			result += "\n";
		}
		return result;

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

	public static String getString(ByteBuffer buffer) {
		try {
			return new String(buffer.array(), 0, buffer.capacity(), "utf-8");
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
			return null;
		}
	}
}
