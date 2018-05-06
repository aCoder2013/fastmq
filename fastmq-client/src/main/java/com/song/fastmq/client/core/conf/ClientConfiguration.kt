package com.song.fastmq.client.core.conf

import java.io.Serializable

/**
 * @author song
 */
class ClientConfiguration : Serializable, Cloneable {

    var brokerServers: String? = null

    var operationTimeoutMs: Long = 30000

    var numOfIoThreads = 1

    var useTcpNoDelay = true

    public override fun clone(): ClientConfiguration {
        try {
            return super.clone() as ClientConfiguration
        } catch (e: CloneNotSupportedException) {
            throw RuntimeException("Failed to clone ClientConfiguration")
        }
    }

    companion object {

        private const val serialVersionUID = 5808234041751981100L
    }

}
