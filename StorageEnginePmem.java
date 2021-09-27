package io.openmessaging;

import org.apache.log4j.Logger;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;

import com.intel.pmem.llpl.MemoryBlock;

public class StorageEnginePmem extends StorageEngine {

	final ArrayList<MemoryBlock> blocks;

	private static final Logger logger = Logger.getLogger(StorageEngine.class);

	StorageEnginePmem() {
		blocks = new ArrayList<>();
	}

	@Override
	public HashMap<Integer, ByteBuffer> getRange(long index, int fetchNum) {
		HashMap<Integer, ByteBuffer> result = new HashMap<>();
		for(long i=0;i<fetchNum;i++){
			MemoryBlock block=blocks.get(index+i);
			result.put(i, );
		}
		return null;
	}

	@Override
	public long write(ByteBuffer buffer) {
		MemoryBlock block = Common.heap.allocateMemoryBlock(buffer.capacity(), false);
		block.copyFromArray(buffer.array(), 0, 0, buffer.capacity());
		blocks.add(block);
		return blocks.size()-1;
	}
}
