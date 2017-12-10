package com.song.fastmq.storage.storage;

import com.song.fastmq.storage.storage.concurrent.AsyncCallbacks;
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

    List<LedgerEntry> readEntries(int maxNumberToRead)
        throws InterruptedException, LedgerStorageException;

    void asyncReadEntries(int maxNumberToRead, AsyncCallbacks.ReadEntryCallback callback);

    void updateReadPosition(Position position);

    void close();
}
