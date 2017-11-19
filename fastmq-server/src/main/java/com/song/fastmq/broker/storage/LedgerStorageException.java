package com.song.fastmq.broker.storage;

/**
 * The class {@link LedgerStorageException} and its subclass are a form of
 * {@code Throwable} that indicates that a ledger storage error happened.
 *
 * Created by song on 2017/11/4.
 */
public class LedgerStorageException extends Exception {

    public LedgerStorageException() {
    }

    public LedgerStorageException(String message) {
        super(message);
    }

    public LedgerStorageException(String message, Throwable cause) {
        super(message, cause);
    }

    public LedgerStorageException(Throwable cause) {
        super(cause);
    }

    public LedgerStorageException(String message, Throwable cause, boolean enableSuppression,
        boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
