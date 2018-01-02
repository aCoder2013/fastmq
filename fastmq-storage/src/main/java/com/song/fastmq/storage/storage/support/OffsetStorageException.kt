package com.song.fastmq.storage.storage.support

/**
 * @author song
 */
class OffsetStorageException : Exception {

    constructor(message: String) : super(message) {}

    constructor(cause: Throwable) : super(cause) {}

    constructor(message: String, cause: Throwable) : super(message, cause) {}
}
