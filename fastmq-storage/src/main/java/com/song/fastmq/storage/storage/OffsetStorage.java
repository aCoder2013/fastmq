package com.song.fastmq.storage.storage;

import com.song.fastmq.storage.storage.concurrent.AsyncCallbacks;
import com.song.fastmq.storage.storage.support.OffsetStorageException;

/**
 * @author song
 */
public interface OffsetStorage {

    void commitOffset(ConsumerInfo consumerInfo, Offset offset);

    Offset queryOffset(ConsumerInfo consumerInfo) throws OffsetStorageException;

    void asyncQueryOffset(ConsumerInfo consumerInfo,
        AsyncCallbacks.ReadOffsetCallback callback);

    void persistOffset(ConsumerInfo consumerInfo) throws InterruptedException;

    void removeOffset(ConsumerInfo consumerInfo);
}
