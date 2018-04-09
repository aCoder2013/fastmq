package com.song.fastmq.broker

import io.netty.util.concurrent.DefaultThreadFactory
import io.reactivex.Observable
import io.reactivex.ObservableEmitter
import io.reactivex.schedulers.Schedulers
import org.junit.Test
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors

/**
 * @author song
 */
class RxJavaTest {


    @Test
    fun just() {
        val latch = CountDownLatch(1)
        Observable.just("test")
            .map {
                println("map->" + Thread.currentThread().name)
                "prefix->$it"
            }
            .subscribeOn(Schedulers.io())
            .observeOn(Schedulers.computation())
            .subscribe {
                println(Thread.currentThread().name)
                latch.countDown()
            }
        latch.await()
    }

    @Test
    fun create() {
        val ioScheduler = Schedulers.from(Executors.newSingleThreadExecutor(DefaultThreadFactory("io-thread-pool")))
        val latch = CountDownLatch(1)
        Observable.create<String> { it: ObservableEmitter<String> ->
            println("create -> ${Thread.currentThread().name}")
            it.onNext("test")
            it.onComplete()
        }
            .observeOn(Schedulers.computation())
            .map {
                println("$it -> ${Thread.currentThread().name}")
            }
            .subscribeOn(ioScheduler)
            .subscribe {
                println("Done ->" + Thread.currentThread().name)
                latch.countDown()
            }
        latch.await()
        ioScheduler.shutdown()
    }
}