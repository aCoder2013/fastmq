package com.song.fastmq.client.utils

import com.google.protobuf.ByteString
import com.song.fastmq.net.proto.BrokerApi
import io.openmessaging.BytesMessage
import java.util.function.Consumer

/**
 * @author song
 */
object MsgUtils {


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

}