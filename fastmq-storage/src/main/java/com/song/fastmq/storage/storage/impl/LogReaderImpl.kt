package com.song.fastmq.storage.storage.impl

import com.song.fastmq.storage.storage.LogReader
import com.song.fastmq.storage.storage.LogRecord
import com.song.fastmq.storage.storage.concurrent.AsyncCallbacks

/**
 * @author song
 */
class LogReaderImpl : LogReader {

    override fun name(): String {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun readEntries(maxNumberToRead: Int): MutableList<LogRecord> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun asyncReadEntries(maxNumberToRead: Int, callback: AsyncCallbacks.ReadEntryCallback?) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun close() {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }
}