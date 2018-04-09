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

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as Offset

        if (ledgerId != other.ledgerId) return false
        if (entryId != other.entryId) return false

        return true
    }

    override fun hashCode(): Int {
        var result = ledgerId.hashCode()
        result = 31 * result + entryId.hashCode()
        return result
    }
}
