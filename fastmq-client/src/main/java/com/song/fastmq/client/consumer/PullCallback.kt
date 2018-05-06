package com.song.fastmq.client.consumer

import com.song.fastmq.client.domain.Message

/**
 * @author song
 */
interface PullCallback {

    fun onSuccess(message: Message)

    fun onThrowable(throwable: Throwable)
}
