package io.openmessaging;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;

abstract public class StoragePage {
	public int dataNumber;
	public int lastOffset;

	public abstract void open();

	public abstract void close();

	public abstract void delete();

	public abstract void write(ByteBuffer buffer) throws IOException;

	public abstract ByteBuffer getDataByIndex(int index) throws IOException;

	public abstract HashMap<Integer, ByteBuffer> getRange(int index, int fetchNum, int preFix);
}
