package com.song.fastmq.storage.storage

/**
 * @author song
 */
class Offset(var ledgerId: Long = 0, var entryId: Long = 0) {




    companion object {
        val NULL_OFFSET = Offset()
    }

    override fun toString(): String {
        return "Offset(ledgerId=$ledgerId, entryId=$entryId)"
    }
}
