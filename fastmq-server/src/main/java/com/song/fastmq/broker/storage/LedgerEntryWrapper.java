package com.song.fastmq.broker.storage;

/**
 * @author song
 */
public interface LedgerEntryWrapper {

    /**
     * @return data of the entry
     */
    byte[] getData();

    /**
     * @return the length of this entry
     */
    int length();

    /**
     * @return the position at which the entry was stored
     */
    Position getPosition();

}
