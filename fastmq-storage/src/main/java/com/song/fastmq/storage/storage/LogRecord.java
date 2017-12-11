package com.song.fastmq.storage.storage;

/**
 * Represent a record stored in BK
 *
 * @author song
 */
public class LogRecord {

    /**
     * Payload the log record
     */
    private final byte[] data;

    /**
     * Offset of the log record
     */
    private final Position position;

    public LogRecord(byte[] data, Position position) {
        this.data = data;
        this.position = position;
    }

    /**
     * @return data of the entry
     */
    public byte[] getData() {
        return this.data;
    }

    /**
     * @return the length of this entry
     */
    public int length() {
        return this.data.length;
    }

    /**
     * @return the position at which the entry was stored
     */
    public Position getPosition() {
        return this.position;
    }

}
