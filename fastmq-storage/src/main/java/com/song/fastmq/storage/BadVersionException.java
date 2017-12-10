package com.song.fastmq.storage;

/**
 * Created by song on 2017/11/5.
 */
public class BadVersionException extends LedgerStorageException {

    public BadVersionException() {
    }

    public BadVersionException(String message) {
        super(message);
    }

    public BadVersionException(String message, Throwable cause) {
        super(message, cause);
    }

    public BadVersionException(Throwable cause) {
        super(cause);
    }

    public BadVersionException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
