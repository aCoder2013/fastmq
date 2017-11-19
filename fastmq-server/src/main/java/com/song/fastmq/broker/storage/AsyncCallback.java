package com.song.fastmq.broker.storage;

/**
 * Created by song on 2017/11/5.
 */
public interface AsyncCallback<T, E> {

    void onCompleted(T result, Version version);

    void onThrowable(E throwable);
}
