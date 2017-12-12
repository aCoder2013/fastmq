package com.song.fastmq.storage.storage.support;

/**
 * @author song
 */
public class OffsetStorageException extends Exception{

    public OffsetStorageException(String message) {
        super(message);
    }

    public OffsetStorageException(Throwable cause) {
        super(cause);
    }

    public OffsetStorageException(String message, Throwable cause) {
        super(message, cause);
    }
}
