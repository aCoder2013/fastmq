package com.song.fastmq.common.utils

import java.util.concurrent.CountDownLatch

/**
 * @author song
 */
class Result<T> {

    private val latch = CountDownLatch(1)

    private var data: T? = null

    var throwable: Throwable? = null
        set(throwable) {
            this.latch.countDown()
            field = throwable
        }

    /**
     * Wait for async operation completes ,and then retrieves its result.
     *
     * @return the data
     * @throws Throwable
     */
    @Throws(Throwable::class)
    fun getData(): T? {
        this.latch.await()
        return data
    }

    /**
     * Only support setData once.
     *
     * @param data
     */
    fun setData(data: T) {
        this.data = data
        this.latch.countDown()
    }
}
