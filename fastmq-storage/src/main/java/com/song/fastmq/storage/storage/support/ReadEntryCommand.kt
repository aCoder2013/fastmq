package com.song.fastmq.storage.storage.support

import com.song.fastmq.storage.storage.LogRecord
import com.song.fastmq.storage.storage.Offset
import com.song.fastmq.storage.storage.concurrent.AsyncCallbacks
import com.song.fastmq.storage.storage.concurrent.AsyncCallbacks.ReadEntryCallback
import com.song.fastmq.storage.storage.impl.LogReaderImpl
import java.util.ArrayList

/**
 * @author song
 */
class ReadEntryCommand(private val ledgerCursor: LogReaderImpl, @field:Volatile var maxNumberToRead: Int,
                       callback: ReadEntryCallback) : AsyncCallbacks.ReadEntryCallback {

    private val entries = ArrayList<LogRecord>()

    val callback: ReadEntryCallback

    val readPosition: Offset
        get() = this.ledgerCursor.readOffset

    init {
        this.callback = callback
    }

    override fun readEntryComplete(entries: List<LogRecord>) {
        if (entries.size > 0) {
            val logRecord = entries[entries.size - 1]
            val offset = logRecord.offset
            this.ledgerCursor
                    .updateReadPosition(
                            Offset(offset.ledgerId, offset.entryId + 1))
            this.entries.addAll(entries)
        }
        callback.readEntryComplete(this.entries)
    }

    fun updateReadPosition(offset: Offset) {
        this.ledgerCursor.updateReadPosition(offset)
    }

    override fun readEntryFailed(throwable: Throwable) {
        callback.readEntryFailed(throwable)
    }

    @Synchronized
    fun addEntries(ledgerEntries: List<LogRecord>) {
        this.entries.addAll(ledgerEntries)
    }

    fun name(): String {
        return this.ledgerCursor.name()
    }
}
