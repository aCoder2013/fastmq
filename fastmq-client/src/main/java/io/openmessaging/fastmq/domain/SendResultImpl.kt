package io.openmessaging.fastmq.domain

import io.openmessaging.KeyValue
import io.openmessaging.OMS
import io.openmessaging.SendResult
import java.util.*

/**
 * @author song
 */
class SendResultImpl(private val messageId: MessageId) : SendResult {

    override fun messageId(): String = Base64.getEncoder().encodeToString(messageId.toByteArray())

    override fun properties(): KeyValue = OMS.newKeyValue()
    override fun toString(): String {
        return "SendResultImpl(messageId=$messageId)"
    }


}