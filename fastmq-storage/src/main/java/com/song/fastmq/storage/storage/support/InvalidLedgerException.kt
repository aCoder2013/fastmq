package com.song.fastmq.storage.storage.support

/**
 * @author song
 */
class InvalidLedgerException : Exception {

    constructor() {}

    constructor(message: String) : super(message) {}

    constructor(message: String, cause: Throwable) : super(message, cause) {}
}
