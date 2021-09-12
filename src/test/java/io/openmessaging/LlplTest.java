package io.openmessaging;

import com.intel.pmem.llpl.Heap;
import com.intel.pmem.llpl.MemoryBlock;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author http://wesleyone.github.io/
 */
public class LlplTest {

	/****************************
	 * This function writes the "Hello..." string to persistent-memory.
	 *****************************/
	public void write_hello_string(byte[] input, Heap h, int size) {
		// block allocation (transactional allocation)
		MemoryBlock newBlock = h.allocateMemoryBlock(size, true);

		// Attached the newBllock to the root address
		h.setRoot(newBlock.handle());

		// Write byte array (input) to newBlock @ offset 0 (on both) for 26 bytes
		newBlock.copyFromArray(input, 0, 0, size);

		// Ensure that the array (input) is in persistent memory
		newBlock.flush();

		// Convert byte array (input) to String format and write to console
		System.out.printf("\nWrite the (%s) string to persistent-memory.\n", new String(input));
		return;
	}

	/****************************
	 * This function reads the "Hello..." string from persistent-memory.
	 *****************************/
	public String read_hello_string(Heap h, int size) {
		// Allocate buffer for string
		// To retrieve byte array from persistent heap
		byte[] output = new byte[size];

		// Get the root block address
		long rootAddr = h.getRoot();
		if (rootAddr == 0) {
			System.out.println("Root Block NOT found!");
			System.exit(0);
		}
		// Map the newBlock to the root of Flushable class
		MemoryBlock newBlock = h.memoryBlockFromHandle(rootAddr);

		// Read 26 bytes @ offset 0 from newBlock to byte array (output)
		newBlock.copyToArray(0L, output, 0, size);

		// Convert byte array (output) to String format and write to console
		String msg = new String(output);
		System.out.printf("\nRead the (%s) string from persistent-memory.\n", msg);
		return msg;
	}

	@Test
	public void main0() {
		String option = "0";
		byte[] input;
		int size; // String length

		// Define Heap
		String path = "./persistent_heap";
		boolean initialized = Heap.exists(path);
		Heap h = initialized ? Heap.openHeap(path) : Heap.createHeap(path, 1024 * 1024 * 16L);

		// Initialize the msg string
		String msg = "Hello Persistent Memory!!!";

		// Convert String to byte array format
		// To store in persistent heap
		input = msg.getBytes();

		// Get the array size
		size = input.length;

		write_hello_string(input, h, size);

		String msgRead = read_hello_string(h, size);

		Assert.assertEquals(msgRead, msg);
	} // End of main function
}