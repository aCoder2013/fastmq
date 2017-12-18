package com.song.fastmq.client

import io.openmessaging.BytesMessage
import io.openmessaging.KeyValue
import io.openmessaging.Message
import io.openmessaging.OMS

/**
 * @author song
 */
class BytesMessageImpl : BytesMessage {

    private val headers: KeyValue = OMS.newKeyValue()

    private val properties: KeyValue = OMS.newKeyValue()

    private var body: ByteArray? = null

    override fun putProperties(key: String, value: Int): Message {
        this.properties.put(key, value)
        return this
    }

    override fun putProperties(key: String, value: Long): Message {
        this.properties.put(key, value)
        return this
    }

    override fun putProperties(key: String, value: Double): Message {
        this.properties.put(key, value)
        return this
    }

    override fun putProperties(key: String, value: String): Message {
        this.properties.put(key, value)
        return this
    }

    override fun setBody(body: ByteArray): BytesMessage {
        this.body = body
        return this
    }

    override fun properties(): KeyValue {
        return this.properties
    }

    override fun putHeaders(key: String, value: Int): Message {
        this.headers.put(key, value)
        return this
    }

    override fun putHeaders(key: String, value: Long): Message {
        this.headers.put(key, value)
        return this
    }

    override fun putHeaders(key: String, value: Double): Message {
        this.headers.put(key, value)
        return this
    }

    override fun putHeaders(key: String, value: String): Message {
        this.headers.put(key, value)
        return this
    }

    override fun headers(): KeyValue {
        return this.headers
    }

    override fun getBody(): ByteArray? {
        return this.body
    }
}
