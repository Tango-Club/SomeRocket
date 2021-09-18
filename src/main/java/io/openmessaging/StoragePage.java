package io.openmessaging;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import org.apache.log4j.Logger;

final public class StoragePage {
	String dataPath;
	String offsetPath;

<<<<<<< HEAD
	MappedByteBuffer dataFile;
	MappedByteBuffer offsetFile;
	RandomAccessFile rdataFile;
	RandomAccessFile roffsetFile;
=======
	RandomAccessFile dataFile;
	RandomAccessFile offsetFile;

	FileChannel dataFileChannel;

>>>>>>> 3e08b3b806ee7dc170e1ea7a04b7dbbac5e21ca8
	int dataNumber;
	int lastOffset;

	boolean isReload;

	private static Logger logger = Logger.getLogger(StorageEngine.class);

<<<<<<< HEAD
	private void unmap(MappedByteBuffer buffer) {
		((sun.nio.ch.DirectBuffer) buffer).cleaner().clean();
		
=======
	public void open() {
		try {
			dataFile = new RandomAccessFile(dataPath, "rw");
			offsetFile = new RandomAccessFile(offsetPath, "rw");
			dataFileChannel = dataFile.getChannel();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}

	public void close() {
		try {
			dataFileChannel.close();
			dataFile.close();
			offsetFile.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
>>>>>>> 3e08b3b806ee7dc170e1ea7a04b7dbbac5e21ca8
	}

	public void flush() throws IOException {
		dataFileChannel.force(false);
		offsetFile.getFD().sync();
	}

	private int getOffsetByIndex(int x) throws IOException {
		offsetFile.seek(x * 4);
		return offsetFile.readInt();
	}

	private void appendOffset(int offset) throws IOException {
		offsetFile.seek(dataNumber * 4);
		offsetFile.writeInt(offset);
	}

	public boolean isReload() {
		return isReload;
	}

<<<<<<< HEAD
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

	public void map() {
		if (isMaped)
			return;
		isMaped = true;
		try {
			rdataFile = new RandomAccessFile(dataPath, "rw");
			roffsetFile = new RandomAccessFile(offsetPath, "rw");
			dataFile = rdataFile.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, Common.pageSize);
			offsetFile = roffsetFile.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, 4 * 1024 * 4);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void unmap() {
		if (!isMaped)
			return;
		isMaped = false;
		try {
			rdataFile.close();
			roffsetFile.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		unmap(dataFile);
		unmap(offsetFile);
	}

	StoragePage(String basePath, boolean exist, int dataNumber) throws IOException {
=======
	StoragePage(String basePath, boolean exist) throws IOException {
>>>>>>> 3e08b3b806ee7dc170e1ea7a04b7dbbac5e21ca8
		this.dataPath = basePath + ".data";
		this.offsetPath = basePath + ".offset";
		Common.initPath(dataPath);
		Common.initPath(offsetPath);

		this.isReload = exist;

		open();

		if (exist) {
			this.dataNumber = (int) offsetFile.length() / 4 - 1;
			this.lastOffset = getOffsetByIndex(dataNumber - 1);
			logger.info("reload: " + dataPath + ", " + offsetPath + ", dataNumber: " + dataNumber);
		} else {
			this.dataNumber = 0;
			this.lastOffset = 0;
			appendOffset(0);
		}
	}

	public synchronized void write(ByteBuffer buffer) throws IOException {
		dataFileChannel.position(lastOffset);
		lastOffset += buffer.capacity();

		dataNumber++;
		dataFileChannel.write(buffer);
		appendOffset(lastOffset);

	}

	public ByteBuffer readNoSeek(int length) throws IOException {
		ByteBuffer buffer = ByteBuffer.allocate(length);
		dataFileChannel.read(buffer);
		buffer.flip();
		return buffer;
	}

	public ByteBuffer getDataByIndexNoSeek(int index) throws IOException {
		int offset = getOffsetByIndex(index);
		int length = getOffsetByIndex(index + 1) - offset;
		return readNoSeek(length);
	}

	public ByteBuffer getDataByIndex(int index) throws IOException {
		int offset = getOffsetByIndex(index);
		dataFileChannel.position(offset);
		int length = getOffsetByIndex(index + 1) - offset;
		return readNoSeek(length);
	}

	public synchronized HashMap<Integer, ByteBuffer> getRange(int index, int fetchNum, int preFix) {
		HashMap<Integer, ByteBuffer> result = new HashMap<Integer, ByteBuffer>();
		try {
			dataFileChannel.position(getOffsetByIndex(index));
			for (int i = 0; i < fetchNum; i++) {
				result.put(preFix + i, getDataByIndexNoSeek(i + index));
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return result;
	}
}
