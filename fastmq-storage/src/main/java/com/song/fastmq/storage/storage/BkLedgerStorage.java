package com.song.fastmq.storage.storage;

import com.song.fastmq.storage.storage.concurrent.AsyncCallbacks.CommonCallback;
import com.song.fastmq.storage.storage.support.LedgerStorageException;

/**
 * A factory to manage ledgers.
 *
 * Created by song on 2017/11/4.
 */
public interface BkLedgerStorage {

	/**
	 * Open a ledger of given name. If the ledger doesn't exist, a new one will be automatically
	 * created.
	 *
	 * @param name name of the ledger,must be unique.
	 * @return the ledger
	 */
	LogManager open(String name) throws LedgerStorageException, InterruptedException;

	void asyncOpen(String name, CommonCallback<LogManager, LedgerStorageException> asyncCallback);

	void close(String name);
}

