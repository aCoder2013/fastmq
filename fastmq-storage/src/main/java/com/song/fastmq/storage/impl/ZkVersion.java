package com.song.fastmq.storage.impl;

import com.song.fastmq.storage.Version;

/**
 * Created by song on 2017/11/5.
 */
public class ZkVersion implements Version {

    private final int version;

    public ZkVersion(int version) {
        this.version = version;
    }

    @Override public int getVersion() {
        return this.version;
    }
}
