package com.song.fastmq.storage;

import com.song.fastmq.storage.concurrent.AsyncCallbacks;
import java.util.List;

/**
 * Use {@link LedgerCursor} to read entries of consumer and persist offset
 *
 * @author song
 */
public interface LedgerCursor {

    /**
     * Get the cursor's name which should be global unique.
     *
     * @return the cursor name
     */
    String name();

    List<LedgerRecord> readEntries(int numberToRead)
        throws InterruptedException, LedgerStorageException;

    void asyncReadEntries(int numberToRead, AsyncCallbacks.ReadEntryCallback callback);

    void close();
}
