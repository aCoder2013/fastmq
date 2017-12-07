package com.song.fastmq.broker.storage.concurrent;

import com.song.fastmq.broker.storage.Version;

/**
 * Created by song on 2017/11/5.
 */
public interface AsyncCallback<T, E> {

    void onCompleted(T data, Version version);

    void onThrowable(E throwable);
}
