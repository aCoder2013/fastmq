package com.song.fastmq.common.utils;

import java.util.concurrent.CountDownLatch;

/**
 * @author song
 */
public class Result<T> {

    private CountDownLatch latch = new CountDownLatch(1);

    private T data;

    private Throwable throwable;

    /**
     * Wait for async operation completes ,and then retrieves its result.
     *
     * @return the data
     * @throws Throwable
     */
    public T getData() throws Throwable {
        this.latch.await();
        return data;
    }

    /**
     * Only support setData once.
     *
     * @param data
     */
    public void setData(T data) {
        this.data = data;
        this.latch.countDown();
    }

    public Throwable getThrowable() {
        return throwable;
    }

    public void setThrowable(Throwable throwable) {
        this.latch.countDown();
        this.throwable = throwable;
    }
}
