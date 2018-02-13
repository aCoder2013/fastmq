package com.song.fastmq.storage.storage

/**
 * @author song
 */
class ConsumerInfo(var consumerName: String = "", var topic: String = "") {

    override fun equals(other: Any?): Boolean {
        if (this === other) {
            return true
        }
        if (other == null || javaClass != other.javaClass) {
            return false
        }

        val that = other as ConsumerInfo?

        return if (topic != that!!.topic) {
            false
        } else consumerName == that.consumerName
    }

    override fun hashCode(): Int {
        var result = topic.hashCode()
        result = 31 * result + consumerName.hashCode()
        return result
    }
}
