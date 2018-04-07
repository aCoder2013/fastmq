package com.song.fastmq.client.consumer

import com.song.fastmq.client.domain.Message

/**
 * @author song
 */
interface PullCallback {

    fun onSuccess(messages: List<Message>)

    fun onThrowable(throwable: Throwable)
}
