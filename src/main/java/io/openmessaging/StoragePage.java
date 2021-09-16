package io.openmessaging;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Field;
import java.lang.reflect.Method;

import org.apache.log4j.Logger;

public class StoragePage {
	String dataPath;
	String offsetPath;

	MappedByteBuffer dataFile;
	MappedByteBuffer offsetFile;
	int dataNumber;
	int lastOffset;

	boolean isReload = false;
	boolean alwaysFlush;
	boolean isMaped=false;

	private static Logger logger = Logger.getLogger(StorageEngine.class);

	private void flush() throws IOException {
		if (!alwaysFlush)
			return;
		dataFile.force();
		offsetFile.force();
	}

	private int getOffsetByIndex(int x) throws IOException {
		offsetFile.position(x * 4);
		return offsetFile.getInt();
	}

	private void appendOffset(int offset) throws IOException {
		offsetFile.position(dataNumber * 4);
		offsetFile.putInt(offset);
	}

	public boolean isReload() {
		return isReload;
	}

	public static void cleanMappedByteBuffer(ByteBuffer buffer) {
		try {
			AccessController.doPrivileged(new PrivilegedExceptionAction<Object>() {
				@Override
				public Object run() throws Exception {
					final Class<?> unsafeClass = Class.forName("sun.misc.Unsafe");
					// we do not need to check for a specific class, we can call the Unsafe method
					// with any buffer class
					MethodHandle unmapper = MethodHandles.lookup().findVirtual(unsafeClass, "invokeCleaner",
							MethodType.methodType(void.class, ByteBuffer.class));
					// fetch the unsafe instance and bind it to the virtual MethodHandle
					final Field f = unsafeClass.getDeclaredField("theUnsafe");
					f.setAccessible(true);
					final Object theUnsafe = f.get(null);
					try {
						unmapper.bindTo(theUnsafe).invokeExact(buffer);
						return null;
					} catch (Throwable t) {
						throw new RuntimeException(t);
					}
				}
			});
		} catch (PrivilegedActionException e) {
			throw new RuntimeException("Unable to unmap the mapped buffer", e);
		}
	}

	public void map() throws FileNotFoundException, IOException {
		if(isMaped)return;
		isMaped=true;
		dataFile = new RandomAccessFile(dataPath, "rw").getChannel().map(FileChannel.MapMode.READ_WRITE, 0, 256 * 1024);
		offsetFile = new RandomAccessFile(offsetPath, "rw").getChannel().map(FileChannel.MapMode.READ_WRITE, 0,
				(Common.pageSize + 1) * 4);
	}

	public void unmap() {
		if(!isMaped)return;
		isMaped=false;
		cleanMappedByteBuffer(dataFile);
		cleanMappedByteBuffer(offsetFile);
	}

	StoragePage(String basePath, boolean exist, boolean alwaysFlush, int dataNumber) throws IOException {
		this.alwaysFlush = alwaysFlush;
		this.dataPath = basePath + ".data";
		this.offsetPath = basePath + ".offset";

		map();

		// logger.info("dataFile: " + dataFile);
		// logger.info("offsetFile: " + dataFile);

		if (exist) {
			this.isReload = true;
			this.dataNumber = dataNumber;
			this.lastOffset = getOffsetByIndex(dataNumber);
			// logger.info("reload: " + dataPath + ", " + offsetPath + ", dataNumber: " +
			// dataNumber);
		} else {
			this.dataNumber = 0;
			this.lastOffset = 0;
			appendOffset(0);
		}
	}

	public synchronized void write(ByteBuffer buffer) throws IOException {
		dataFile.position(lastOffset);
		lastOffset += buffer.capacity();

		dataNumber++;
		dataFile.put(buffer);
		appendOffset(lastOffset);

		flush();
	}

	public ByteBuffer readNoSeek(int length) throws IOException {
		ByteBuffer buffer = ByteBuffer.allocate(length);
		byte[] data = new byte[length];
		dataFile.get(data);
		buffer.put(data);
		buffer.flip();
		return buffer;
	}

	public ByteBuffer getDataByIndexNoSeek(int index) throws IOException {
		int offset = getOffsetByIndex(index);
		int length = getOffsetByIndex(index + 1) - offset;
		// logger.info(getOffsetByIndex(index)+","+getOffsetByIndex(index + 1));
		return readNoSeek(length);
	}

	public ByteBuffer getDataByIndex(int index) throws IOException {
		int offset = getOffsetByIndex(index);
		dataFile.position(offset);
		int length = getOffsetByIndex(index + 1) - offset;
		return readNoSeek(length);
	}

	public synchronized HashMap<Integer, ByteBuffer> getRange(int index, int fetchNum, int preFix) {
		HashMap<Integer, ByteBuffer> result = new HashMap<Integer, ByteBuffer>();
		try {
			dataFile.position(getOffsetByIndex(index));
			for (int i = 0; i < fetchNum; i++) {
				result.put(preFix + i, getDataByIndexNoSeek(i + index));
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return result;
	}
}
