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
		MemoryAccessor.uncheckedCopyFromArray(srcArray, srcOffset, directAddress() + metadataSize() + dstOffset, length);
    }

	public void copyToArray(long srcAddress, byte[] dstArray, int dstOffset, int length) {
        long dstAddress = AnyHeap.UNSAFE.ARRAY_BYTE_BASE_OFFSET + AnyHeap.UNSAFE.ARRAY_BYTE_INDEX_SCALE * dstOffset;
        AnyHeap.UNSAFE.copyMemory(null, srcAddress, dstArray, dstAddress, length);
    }
}
