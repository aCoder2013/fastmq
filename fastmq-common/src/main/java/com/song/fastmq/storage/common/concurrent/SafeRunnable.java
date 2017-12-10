package com.song.fastmq.storage.common.concurrent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by song on 2017/11/5.
 */
public abstract class SafeRunnable implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(SafeRunnable.class);

    @Override public void run() {
        try {
            safeRun();
        } catch (Throwable e) {
            logger.error("Unknown exception", e);
        }
    }

    public abstract void safeRun();
}


