package com.song.fastmq.storage.storage.utils

import org.apache.bookkeeper.util.SafeRunnable
import java.util.function.Consumer

/**
 * @author song
 */
object SafeRun {

    fun safeRun(runnable: Runnable): SafeRunnable {
        return object : SafeRunnable() {
            override fun safeRun() {
                runnable.run()
            }
        }
    }

    /**
     *
     * @param runnable
     * @param exceptionHandler
     * handler that will be called when there are any exception
     * @return
     */
    fun safeRun(runnable: Runnable, exceptionHandler: Consumer<Throwable>): SafeRunnable {
        return object : SafeRunnable() {
            override fun safeRun() {
                try {
                    runnable.run()
                } catch (t: Throwable) {
                    exceptionHandler.accept(t)
                    throw t
                }

            }
        }
    }

}
