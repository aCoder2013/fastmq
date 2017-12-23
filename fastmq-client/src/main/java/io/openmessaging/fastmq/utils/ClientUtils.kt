package io.openmessaging.fastmq.utils

import com.google.protobuf.ByteString
import com.song.fastmq.net.proto.BrokerApi
import com.song.fastmq.storage.common.utils.Utils
import io.openmessaging.BytesMessage
import io.openmessaging.KeyValue
import io.openmessaging.OMS
import java.util.concurrent.atomic.AtomicLong
import java.util.function.Consumer

/**
 * @author song
 */
object ClientUtils {

    private val producerIdGenerator = AtomicLong(System.currentTimeMillis())

    private val consumerIdGenerator = AtomicLong(System.currentTimeMillis())

    fun getNextProducerId(): Long {
        return producerIdGenerator.incrementAndGet()
    }

    fun getNextConsumerId(): Long {
        return consumerIdGenerator.incrementAndGet()
    }

    fun buildInstanceName(): String {
        val localAddress = Utils.getLocalAddress()
        localAddress.let {
            return it + "@" + Utils.getPid()
        }
    }

    /**
     * Convert OMS message into protocol-buffers message
     */
    fun msgConvert(message: BytesMessage): BrokerApi.Message {
        val builder = BrokerApi.Message.newBuilder()
        message.headers().keySet().forEach(Consumer {
            builder.putHeaders(it, message.headers().getString(it))
        })
        message.properties().keySet().forEach(Consumer {
            builder.putProperties(it, message.headers().getString(it))
        })
        builder.body = ByteString.copyFrom(message.body)
        return builder.build()
    }

    fun buildKeyValue(vararg keyValues: KeyValue): KeyValue {
        val keyValue = OMS.newKeyValue()
        for (properties in keyValues) {
            for (key in properties.keySet()) {
                keyValue.put(key, properties.getString(key))
            }
        }
        return keyValue
    }

}