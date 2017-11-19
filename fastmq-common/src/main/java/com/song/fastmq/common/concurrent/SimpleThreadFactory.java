package com.song.fastmq.common.concurrent;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.concurrent.ThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by song on 2017/11/5.
 */
public class SimpleThreadFactory implements ThreadFactory {

    private static final Logger logger = LoggerFactory.getLogger(SimpleThreadFactory.class);

    private ThreadFactory threadFactory;

    public SimpleThreadFactory(String name) {
        ThreadFactoryBuilder builder = new ThreadFactoryBuilder();
        builder
            .setNameFormat(name)
            .setUncaughtExceptionHandler((t, e) -> logger.error("Uncaught exception", e));
        this.threadFactory = builder.build();
    }

    @Override public Thread newThread(Runnable r) {
        return threadFactory.newThread(r);
    }
}
