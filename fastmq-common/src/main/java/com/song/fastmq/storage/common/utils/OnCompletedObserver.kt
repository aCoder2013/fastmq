package com.song.fastmq.storage.common.utils

import io.reactivex.Observer
import io.reactivex.disposables.Disposable

/**
 * @author song
 */
abstract class OnCompletedObserver<T> : Observer<T> {

    override fun onNext(t: T) {
    }

    override fun onSubscribe(d: Disposable) {

    }

    override fun onComplete() {

    }
}