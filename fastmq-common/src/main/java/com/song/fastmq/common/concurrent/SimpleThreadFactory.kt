package com.song.fastmq.common.concurrent

import com.google.common.util.concurrent.ThreadFactoryBuilder
import org.slf4j.LoggerFactory
import java.util.concurrent.ThreadFactory

/**
 * Created by song on 2017/11/5.
 */
class SimpleThreadFactory(name: String) : ThreadFactory {

    private val threadFactory: ThreadFactory

    init {
        val builder = ThreadFactoryBuilder()
        builder
            .setNameFormat(name)
            .setUncaughtExceptionHandler { t, e -> logger.error("Uncaught exception of thread_" + t.toString(), e) }
        this.threadFactory = builder.build()
    }

    override fun newThread(r: Runnable): Thread {
        return threadFactory.newThread(r)
    }

    companion object {

        private val logger = LoggerFactory.getLogger(SimpleThreadFactory::class.java)
    }
}
