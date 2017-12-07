package com.song.fastmq.broker.storage.impl;

/**
 * @author song
 */
public class InvalidLedgerException extends Exception {

    public InvalidLedgerException() {
    }

    public InvalidLedgerException(String message) {
        super(message);
    }

    public InvalidLedgerException(String message, Throwable cause) {
        super(message, cause);
    }
}
