package com.song.fastmq.broker.exception;

/**
 * @author song
 */
public class TopicIllegalStateException extends RuntimeException {

    public TopicIllegalStateException() {
    }

    public TopicIllegalStateException(String message) {
        super(message);
    }

    public TopicIllegalStateException(String message, Throwable cause) {
        super(message, cause);
    }

    public TopicIllegalStateException(Throwable cause) {
        super(cause);
    }

    public TopicIllegalStateException(String message, Throwable cause, boolean enableSuppression,
        boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
