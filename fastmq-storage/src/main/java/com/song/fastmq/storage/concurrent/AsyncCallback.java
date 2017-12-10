package com.song.fastmq.storage.concurrent;

import com.song.fastmq.storage.Version;

/**
 * Created by song on 2017/11/5.
 */
public interface AsyncCallback<T, E> {

    void onCompleted(T data, Version version);

    void onThrowable(E throwable);
}
