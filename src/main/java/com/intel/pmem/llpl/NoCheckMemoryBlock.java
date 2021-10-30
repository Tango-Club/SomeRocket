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

	public void copyFromArray(byte[] srcArray, int srcOffset, long dstOffset, int length) {
		MemoryAccessor.uncheckedCopyFromArray(srcArray, srcOffset, directAddress() + dstOffset, length);
	}

	public void copyToArray(long srcOffset, byte[] dstArray, int dstOffset, int length) {
		uncheckedCopyToArray(directAddress() + srcOffset, dstArray, dstOffset, length);
	}
}
