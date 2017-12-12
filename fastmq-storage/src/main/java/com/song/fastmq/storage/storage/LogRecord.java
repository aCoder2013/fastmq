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
    private final Offset offset;

    public LogRecord(byte[] data, Offset offset) {
        this.data = data;
        this.offset = offset;
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
     * @return the offset at which the entry was stored
     */
    public Offset getOffset() {
        return this.offset;
    }

}
