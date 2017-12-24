package io.openmessaging.fastmq.concurrent

import io.openmessaging.Promise
import io.openmessaging.PromiseListener
import io.openmessaging.exception.OMSRuntimeException
import org.slf4j.LoggerFactory
import java.util.*

/**
 * @author song
 */
class DefaultPromise<V> : Promise<V> {

    private val LOG = LoggerFactory.getLogger(DefaultPromise::class.java)

    private val lock = java.lang.Object()

    @Volatile
    private var state = FutureState.DOING

    private var result: V? = null

    private var timeout: Long = 0L

    private var createTime: Long = 0L

    private var exception: Throwable? = null

    private var promiseListenerList: MutableList<PromiseListener<V>>? = null

    init {
        createTime = System.currentTimeMillis()
        promiseListenerList = ArrayList()
        timeout = 1000000000
    }

    override fun cancel(mayInterruptIfRunning: Boolean): Boolean {
        return false
    }

    override fun isCancelled(): Boolean {
        return state.isCancelledState()
    }

    override fun isDone(): Boolean {
        return state.isDoneState()
    }

    override fun get(): V? {
        return result
    }

    override fun get(timeout: Long): V? {
        synchronized(lock) {
            if (!isDoing()) {
                return getValueOrThrowable()
            }

            if (timeout <= 0) {
                try {
                    lock.wait()
                } catch (e: Exception) {
                    cancel(e)
                }

                return getValueOrThrowable()
            } else {
                var waitTime = timeout - (System.currentTimeMillis() - createTime)
                if (waitTime > 0) {
                    while (true) {
                        try {
                            lock.wait(waitTime)
                        } catch (e: InterruptedException) {
                            LOG.error("promise get value interrupted,excepiton:{}", e.message)
                        }

                        if (!isDoing()) {
                            break
                        } else {
                            waitTime = timeout - (System.currentTimeMillis() - createTime)
                            if (waitTime <= 0) {
                                break
                            }
                        }
                    }
                }

                if (isDoing()) {
                    timeoutSoCancel()
                }
            }
            return getValueOrThrowable()
        }
    }

    override fun set(value: V?): Boolean {
        if (value == null)
            return false
        this.result = value
        return done()
    }

    override fun setFailure(cause: Throwable?): Boolean {
        if (cause == null)
            return false
        this.exception = cause
        return done()
    }

    override fun addListener(listener: PromiseListener<V>?) {
        if (listener == null) {
            throw NullPointerException("FutureListener is null")
        }

        var notifyNow = false
        synchronized(lock) {
            if (!isDoing()) {
                notifyNow = true
            } else {
                if (promiseListenerList == null) {
                    promiseListenerList = ArrayList()
                }
                promiseListenerList!!.add(listener)
            }
        }

        if (notifyNow) {
            notifyListener(listener)
        }
    }

    override fun getThrowable(): Throwable? {
        return exception
    }

    private fun notifyListeners() {
        if (promiseListenerList != null) {
            for (listener in promiseListenerList!!) {
                notifyListener(listener)
            }
        }
    }

    private fun isSuccess(): Boolean {
        return isDone && exception == null
    }

    private fun timeoutSoCancel() {
        synchronized(lock) {
            if (!isDoing()) {
                return
            }
            state = FutureState.CANCELLED
            exception = RuntimeException("Get request result is timeout or interrupted")
            lock.notifyAll()
        }
        notifyListeners()
    }

    private fun getValueOrThrowable(): V? {
        if (exception != null) {
            val e = if (exception!!.cause != null) exception!!.cause else exception
            throw OMSRuntimeException("-1", e)
        }
        notifyListeners()
        return result
    }

    private fun isDoing(): Boolean {
        return state.isDoingState()
    }

    private fun done(): Boolean {
        synchronized(lock) {
            if (!isDoing()) {
                return false
            }

            state = FutureState.DONE
            lock.notifyAll()
        }

        notifyListeners()
        return true
    }

    private fun notifyListener(listener: PromiseListener<V>) {
        try {
            if (exception != null)
                listener.operationFailed(this)
            else
                listener.operationCompleted(this)
        } catch (t: Throwable) {
            LOG.error("notifyListener {} Error:{}", listener.javaClass.simpleName, t)
        }
    }

    private fun cancel(e: Exception): Boolean {
        synchronized(lock) {
            if (!isDoing()) {
                return false
            }

            state = FutureState.CANCELLED
            exception = e
            lock.notifyAll()
        }

        notifyListeners()
        return true
    }

}