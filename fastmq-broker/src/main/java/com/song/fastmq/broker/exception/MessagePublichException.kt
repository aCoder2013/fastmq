package com.song.fastmq.broker.exception

/**
 * @author song
 */
class MessagePublichException : Exception {

    constructor(message: String) : super(message)

    constructor(cause: Throwable) : super(cause)

    constructor(message: String, cause: Throwable) : super(message, cause)


}