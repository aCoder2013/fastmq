package com.song.fastmq.broker.exception

import java.io.IOException

/**
 * @author song
 */
class FastMQServiceException : IOException {
    
    constructor(message: String) : super(message)

    constructor(message: String, cause: Throwable) : super(message, cause)
}