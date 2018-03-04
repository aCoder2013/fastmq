package com.song.fastmq.common.message

import java.util.*

/**
 * @author song
 */
class Message(var messageId: MessageId = MessageId.EMPTY,
              val data: ByteArray, var properties: Map<String, String> = Collections.emptyMap()) {


}