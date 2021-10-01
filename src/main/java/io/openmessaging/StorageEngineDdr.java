package io.openmessaging;

import org.apache.log4j.Logger;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import sun.nio.ch.DirectBuffer;

public class StorageEngineDdr extends StorageEngine {

	final ArrayList<ByteBuffer> blocks;

	private static final Logger logger = Logger.getLogger(StorageEngineDdr.class);

	StorageEngineDdr() {
		blocks = new ArrayList<>();
	}

	@Override
	public HashMap<Integer, ByteBuffer> getRange(long index, int fetchNum) {
		HashMap<Integer, ByteBuffer> result = new HashMap<>();
		fetchNum = Math.min(fetchNum, (int) (blocks.size() - index));
		for (int i = 0; i < fetchNum; i++) {
			ByteBuffer block = blocks.get((int) (index + i));
			ByteBuffer buffer = ByteBuffer.allocate(block.capacity());
			buffer.put(block.array());
			buffer.flip();
			((DirectBuffer) block).cleaner().clean();
			result.put(i, buffer);
		}
		return result;
	}

	@Override
	public long write(ByteBuffer buffer) {
		ByteBuffer block = ByteBuffer.allocateDirect(buffer.remaining());
		block.put(buffer);
		blocks.add(block);
		return blocks.size() - 1;
	}
}
