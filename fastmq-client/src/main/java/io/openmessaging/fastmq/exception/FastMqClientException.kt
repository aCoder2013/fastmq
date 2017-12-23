package io.openmessaging.fastmq.exception

import java.io.IOException

/**
 * @author song
 */
class FastMqClientException : IOException {

    constructor(cause: Throwable?) : super()

    constructor(msg: String) : super(msg)
}