package com.song.fastmq.storage.storage;

import com.song.fastmq.storage.storage.concurrent.AsyncCallbacks;
import com.song.fastmq.storage.storage.support.LedgerStorageException;
import java.util.List;

/**
 * Use {@link LogReader} to read entries of consumer and persist offset
 *
 * @author song
 */
public interface LogReader {

    /**
     * Get the cursor's name which should be global unique.
     *
     * @return the cursor name
     */
    String name();

    List<LogRecord> readEntries(int maxNumberToRead)
        throws InterruptedException, LedgerStorageException;

    void asyncReadEntries(int maxNumberToRead, AsyncCallbacks.ReadEntryCallback callback);

    void close();
}
