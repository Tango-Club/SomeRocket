package io.openmessaging;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;

public class StorageEngineEssd {
	private static final Logger logger = Logger.getLogger(StorageEngineEssd.class);

	final String storagePath;
	final ArrayList<Long> dataNumPre = new ArrayList<>();
	final ArrayList<StoragePageEssd> pages = new ArrayList<>();

	public StorageEngineEssd(String topic, int queueId, String basePath) {
		Common.initDirectory(basePath);
		String storagePath = basePath + "/ds_" + topic + "_" + queueId;
		this.storagePath = storagePath;
		Common.initDirectory(storagePath);
		dataNumPre.add(0L);
	}

	private void updateDataNum() {
		dataNumPre.set(dataNumPre.size() - 1, getDataNum() + 1);
	}

	private Long getDataNum() {
		return dataNumPre.get(dataNumPre.size() - 1);
	}

	private StoragePageEssd getLastPage() {
		return pages.get(pages.size() - 1);
	}

	public long write(ByteBuffer buffer) {
		if (pages.size() == 0 || getLastPage().lastOffset + buffer.remaining() > Common.pageSize) {
			if (pages.size() != 0) {
				getLastPage().close();
			}
			String pagePath = storagePath + "/" + pages.size();
			try {
				pages.add(new StoragePageEssd(pagePath, false));
			} catch (IOException e) {
				e.printStackTrace();
			}
			dataNumPre.add(getDataNum());
		}
		try {
			getLastPage().write(buffer);
		} catch (IOException e) {
			e.printStackTrace();
		}
		updateDataNum();
		return getDataNum() - 1;
	}

	public HashMap<Integer, ByteBuffer> getRange(long index, int fetchNum) {
		fetchNum = (int) Math.min(fetchNum, getDataNum() - index);
		HashMap<Integer, ByteBuffer> result = new HashMap<>();
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

			if (pageIndex == pages.get(pageId).dataNumber && pageId + 1 < pages.size()) {
				pages.get(pageId).delete();
			}

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
