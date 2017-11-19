package com.song.fastmq.broker.storage.config;

import com.google.common.base.Charsets;
import org.apache.bookkeeper.client.BookKeeper;

/**
 * Created by song on 2017/11/5.
 */
public class BookKeeperConfig {

    private int ensSize = 3;

    private int writeQuorumSize = 2;

    private BookKeeper.DigestType digestType = BookKeeper.DigestType.MAC;
    private byte[] password = "".getBytes(Charsets.UTF_8);

    public int getEnsSize() {
        return ensSize;
    }

    public void setEnsSize(int ensSize) {
        this.ensSize = ensSize;
    }

    public int getWriteQuorumSize() {
        return writeQuorumSize;
    }

    public void setWriteQuorumSize(int writeQuorumSize) {
        this.writeQuorumSize = writeQuorumSize;
    }

    public BookKeeper.DigestType getDigestType() {
        return digestType;
    }

    public void setDigestType(BookKeeper.DigestType digestType) {
        this.digestType = digestType;
    }

    public byte[] getPassword() {
        return password;
    }

    public void setPassword(byte[] password) {
        this.password = password;
    }
}
