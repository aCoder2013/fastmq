package com.song.fastmq.storage.storage.concurrent;

import com.song.fastmq.storage.common.concurrent.SafeRunnable;
import com.song.fastmq.storage.common.concurrent.SimpleThreadFactory;
import com.song.fastmq.storage.common.utils.Utils;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Created by song on 2017/11/5.
 */
public class CommonPool {

    private static final ThreadPoolExecutor COMMON_POOL = new ThreadPoolExecutor(
        Utils.INSTANCE.getAVAILABLE_PROCESSORS(), Utils.INSTANCE.getAVAILABLE_PROCESSORS() * 4,
        120L, TimeUnit.SECONDS, new LinkedBlockingQueue<>(),
        new SimpleThreadFactory("common-pool"));

    public static void executeBlocking(Runnable runnable) {
        COMMON_POOL.execute(new SafeRunnable() {

            @Override
            public void safeRun() {
                runnable.run();
            }
        });
    }

}
