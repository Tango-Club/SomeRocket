package io.openmessaging;

import java.nio.ByteBuffer;
import java.util.HashMap;

public abstract class StorageEngine {

	public abstract long write(ByteBuffer buffer);

	public abstract HashMap<Integer, ByteBuffer> getRange(long index, int fetchNum);
}
