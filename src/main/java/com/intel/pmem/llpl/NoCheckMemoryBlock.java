package com.intel.pmem.llpl;

public final class NoCheckMemoryBlock extends AbstractMemoryBlock {
	public NoCheckMemoryBlock(Heap heap, long size, boolean transactional) {
		super(heap, size, false, transactional);
	}

	@Override
	long metadataSize() {
		return 0;
	}

	@Override
	void checkBounds(long offset, long length) {
	}

	@Override
	void checkBoundsAndLength(long offset, long length) {
	}

}
