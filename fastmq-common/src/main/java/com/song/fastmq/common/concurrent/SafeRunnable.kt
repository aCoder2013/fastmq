package com.song.fastmq.common.concurrent

import org.slf4j.LoggerFactory

/**
 * Created by song on 2017/11/5.
 */
abstract class SafeRunnable : Runnable {

    override fun run() {
        try {
            safeRun()
        } catch (e: Throwable) {
            logger.error("Uncaught exception", e)
        }
    }

    abstract fun safeRun()

    companion object {

        private val logger = LoggerFactory.getLogger(SafeRunnable::class.java)
    }
}


