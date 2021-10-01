package io.openmessaging;

import org.apache.log4j.Logger;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;

import com.intel.pmem.llpl.MemoryBlock;

public class StorageEnginePmem extends StorageEngine {

	final ArrayList<MemoryBlock> blocks;

	private static final Logger logger = Logger.getLogger(StorageEnginePmem.class);

	StorageEnginePmem() {
		blocks = new ArrayList<>();
	}

	@Override
	public HashMap<Integer, ByteBuffer> getRange(long index, int fetchNum) {
		HashMap<Integer, ByteBuffer> result = new HashMap<>();
		fetchNum = Math.min(fetchNum, (int) (blocks.size() - index));
		for (int i = 0; i < fetchNum; i++) {
			MemoryBlock block = blocks.get((int) (index + i));
			ByteBuffer buffer = ByteBuffer.allocate((int) block.size());
			block.copyToArray(0, buffer.array(), 0, (int) block.size());
			result.put(i, buffer);
		}
		return result;
	}

	@Override
	public long write(ByteBuffer buffer) {
		MemoryBlock block = Common.heap.allocateMemoryBlock(buffer.remaining(), false);
		block.copyFromArray(buffer.array(), 0, 0, buffer.remaining());
		blocks.add(block);
		return blocks.size() - 1;
	}
}
