package com.song.fastmq.broker.exception;

/**
 * @author song
 */
public class TopicClosedException extends RuntimeException{

    public TopicClosedException() {
    }

    public TopicClosedException(String message) {
        super(message);
    }

    public TopicClosedException(String message, Throwable cause) {
        super(message, cause);
    }

    public TopicClosedException(Throwable cause) {
        super(cause);
    }

    public TopicClosedException(String message, Throwable cause, boolean enableSuppression,
        boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
