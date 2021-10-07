package io.openmessaging;

import org.apache.log4j.Logger;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;

public class StorageEngineDdr extends StorageEngine {

	final ArrayList<ByteBuffer> blocks;
	boolean isDirect;

	private static final Logger logger = Logger.getLogger(StorageEngineDdr.class);

	StorageEngineDdr(boolean isOutHeapMemory) {
		blocks = new ArrayList<>();
		isDirect = isOutHeapMemory;
	}

	@Override
	public HashMap<Integer, ByteBuffer> getRange(long index, int fetchNum) {
		HashMap<Integer, ByteBuffer> result = new HashMap<>();
		fetchNum = Math.min(fetchNum, (int) (blocks.size() - index));
		for (int i = 0; i < fetchNum; i++) {
			result.put(i, blocks.get((int) (index + i)));
			blocks.set((int) (index + i), null);
		}
		return result;
	}

	@Override
	public long write(ByteBuffer buffer) {
		blocks.add(Common.clone(buffer, isDirect));
		return blocks.size() - 1;
	}
}
