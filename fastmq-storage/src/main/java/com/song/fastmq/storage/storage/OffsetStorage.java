package com.song.fastmq.storage.storage;

import com.song.fastmq.storage.storage.concurrent.AsyncCallbacks;
import com.song.fastmq.storage.storage.support.OffsetStorageException;

/**
 * @author song
 */
public interface OffsetStorage {

    void commitOffset(LogReaderInfo logReaderInfo, Offset offset);

    Offset queryOffset(LogReaderInfo logReaderInfo) throws OffsetStorageException;

    void asyncQueryOffset(LogReaderInfo logReaderInfo,
        AsyncCallbacks.ReadOffsetCallback callback);

    void persistOffset(LogReaderInfo logReaderInfo) throws InterruptedException;

    void removeOffset(LogReaderInfo logReaderInfo);
}
