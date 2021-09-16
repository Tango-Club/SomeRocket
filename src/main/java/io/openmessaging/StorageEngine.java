package io.openmessaging;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.log4j.Logger;

public class StorageEngine {
	String storagePath;

	boolean isReload = false;
	boolean alwaysFlush;

	RandomAccessFile metaFile;
	ArrayList<Long> dataNumPre = new ArrayList<Long>();
	ArrayList<StoragePage> pages = new ArrayList<StoragePage>();

	private static Logger logger = Logger.getLogger(StorageEngine.class);

	public boolean isReload() {
		return isReload;
	}

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
		metaFile.seek(pageId * 4);
		return metaFile.readInt();
	}

	void setPageDataNum(int pageId, int dataNum) throws IOException {
		metaFile.seek(pageId * 4);
		metaFile.writeInt(dataNum);
	}

	StorageEngine(String storagePath, boolean exist, boolean alwaysFlush) throws IOException {
		this.alwaysFlush = alwaysFlush;
		this.storagePath = storagePath;

		dataNumPre.add(0L);
		if (exist) {
			isReload = true;
			metaFile = new RandomAccessFile(storagePath + "/meta", "rwd");
			metaFile.seek(0);
			for (int pageId = 0; pageId < metaFile.length() / 4; pageId++) {
				String pagePath = storagePath + "/" + Integer.toString(pageId);
				pages.add(new StoragePage(pagePath, true, alwaysFlush, metaFile.readInt()));
				dataNumPre.add(getDataNum() + getLastPage().dataNumber);
			}
			// logger.info("reload: pageNum = " + pages.size() + ", dataNum = " +
			// getDataNum());
		} else {
			Common.initPath(storagePath + "/meta");
			metaFile = new RandomAccessFile(storagePath + "/meta", "rwd");
		}
	}

	public synchronized long write(ByteBuffer buffer) throws IOException {
		// if (pages.size() != 0)
		// logger.info("page limit: " + getLastPage().dataNumber + "," +
		// Common.pageSize);
		if (pages.size() == 0 || getLastPage().dataNumber == Common.pageSize) {
			if (!pages.isEmpty())
				getLastPage().unmap();
			String pagePath = storagePath + "/" + Integer.toString(pages.size());
			pages.add(new StoragePage(pagePath, false, alwaysFlush, 0));
			dataNumPre.add(getDataNum());
			metaFile.seek(metaFile.length());
			metaFile.writeInt(0);
		}
		setPageDataNum(pages.size() - 1, getPageDataNum(pages.size() - 1) + 1);
		getLastPage().write(buffer);
		updateDataNum();
		return getDataNum() - 1;
	}

	public synchronized HashMap<Integer, ByteBuffer> getRange(long index, int fetchNum) {
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
		// logger.info("binary serch: pageId = " + pageId + ", pageIndex = " +
		// pageIndex);
		int readed = 0;
		while (fetchNum > 0) {
			int pageFetchNum = Math.min(fetchNum, pages.get(pageId).dataNumber - pageIndex);
			// logger.info("page fetch: " + pageFetchNum);
			try {
				pages.get(pageId).map();
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
			result.putAll(pages.get(pageId).getRange(pageIndex, pageFetchNum, readed));
			pageIndex += pageFetchNum;
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
