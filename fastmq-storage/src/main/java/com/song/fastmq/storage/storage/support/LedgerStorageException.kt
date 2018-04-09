package com.song.fastmq.storage.storage.support

/**
 * The class [LedgerStorageException] and its subclass are a form of
 * `Throwable` that indicates that a ledger storage error happened.
 *
 * Created by song on 2017/11/4.
 */
open class LedgerStorageException : Exception {

    constructor()

    constructor(message: String) : super(message)

    constructor(message: String, cause: Throwable) : super(message, cause)

    constructor(cause: Throwable) : super(cause)

    constructor(
        message: String, cause: Throwable, enableSuppression: Boolean,
        writableStackTrace: Boolean
    ) : super(message, cause, enableSuppression, writableStackTrace)
}
