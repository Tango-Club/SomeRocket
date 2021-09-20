package io.openmessaging;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.io.FileNotFoundException;
import org.apache.log4j.Logger;

public class StorageEngineSynced {
	String dataPath;
	String offsetPath;
	String qidPath;

	RandomAccessFile dataFile;
	RandomAccessFile offsetFile;
	RandomAccessFile qidFile;

	FileChannel dataFileChannel;

	private static Logger logger = Logger.getLogger(StorageEngine.class);

	void updateDataNum() {
		dataNumPre.set(dataNumPre.size() - 1, getDataNum() + 1);
	}

	Long getDataNum() {
		return dataNumPre.get(dataNumPre.size() - 1);
	}

	StoragePage getLastPage() {
		return pages.get(pages.size() - 1);
	}

	int getPageDataNum(int pageId) throws IOException {
		return (int) pages.get(pageId).offsetFile.length() / 4;
	}

	StorageEngineSynced(String storagePath, boolean exist) throws IOException {
		this.dataPath = storagePath + "/0.data";
		this.offsetPath = storagePath + "/0.offset";
		this.qidPath = storagePath + "/0.qid";

		Common.initPath(this.dataPath);
		Common.initPath(this.offsetPath);
		Common.initPath(this.qidPath);

		try {
			dataFile = new RandomAccessFile(dataPath, "rw");
			offsetFile = new RandomAccessFile(offsetPath, "rw");
			qidFile = new RandomAccessFile(qidPath, "rw");

			dataFileChannel = dataFile.getChannel();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}

		if (exist) {

		}
	}

	public long write(ByteBuffer buffer) throws IOException {
		if (pages.size() == 0 || getLastPage().lastOffset + buffer.capacity() > Common.pageSize) {
			if (pages.size() != 0) {
				getLastPage().close();
			}
			String pagePath = storagePath + "/" + Integer.toString(pages.size());
			pages.add(new StoragePage(pagePath, false));
			dataNumPre.add(getDataNum());
		}
		getLastPage().write(buffer);
		updateDataNum();
		return getDataNum() - 1;
	}

	public HashMap<Integer, ByteBuffer> getRange(long index, int fetchNum) {
		fetchNum = (int) Math.min((long) fetchNum, getDataNum() - index);
		HashMap<Integer, ByteBuffer> result = new HashMap<Integer, ByteBuffer>();
		if (fetchNum <= 0)
			return result;

		int left = 1, right = pages.size();
		while (left < right) {
			int mid = (left + right) / 2;
			if (dataNumPre.get(mid) <= index) {
				left = mid + 1;
			} else {
				right = mid;
			}
		}

		int pageId = left - 1;
		int pageIndex = (int) (index - dataNumPre.get(pageId));
		int readed = 0;
		while (fetchNum > 0) {
			int pageFetchNum = Math.min(fetchNum, pages.get(pageId).dataNumber - pageIndex);
			if (pageFetchNum == 0)
				break;

			if (pageId != pages.size() - 1)
				pages.get(pageId).open();
			result.putAll(pages.get(pageId).getRange(pageIndex, pageFetchNum, readed));
			pageIndex += pageFetchNum;
			if (pageId != pages.size() - 1)
				pages.get(pageId).close();

			if (pageIndex == pages.get(pageId).dataNumber) {
				pageIndex = 0;
				pageId++;
			}
			fetchNum -= pageFetchNum;
			readed += pageFetchNum;
		}
		return result;
	}
}
