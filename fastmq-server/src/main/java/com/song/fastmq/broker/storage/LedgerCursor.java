package com.song.fastmq.broker.storage;

import com.song.fastmq.broker.storage.concurrent.AsyncCallbacks;
import java.util.List;

/**
 * @author song
 */
public interface LedgerCursor {

    /**
     * Get the cursor's name which should be global unique.
     *
     * @return the cursor name
     */
    String name();

    List<LedgerEntryWrapper> readEntries(int numberToRead)
        throws InterruptedException, LedgerStorageException;

    void asyncReadEntries(int numberToRead, AsyncCallbacks.ReadEntryCallback callback);

    void close();

    void asyncClose(AsyncCallbacks.CloseLedgerCursorCallback callback);
}
