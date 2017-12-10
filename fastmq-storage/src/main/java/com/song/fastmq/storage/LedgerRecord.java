package com.song.fastmq.storage;

/**
 * @author song
 */
public class LedgerRecord {

    private final byte[] data;

    private final Position position;

    public LedgerRecord(byte[] data, Position position) {
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
