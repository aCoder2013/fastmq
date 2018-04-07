package com.song.fastmq.net.proto

import com.song.fastmq.net.proto.BrokerApi.*

/**
 * @author song
 */
object Commands {

    fun newProducer(topic: String, producerId: Long, producerName: String, requestId: Long): Command {
        val producer = BrokerApi.CommandProducer
                .newBuilder()
                .setProducerId(producerId)
                .setProducerName(producerName)
                .setTopic(topic)
                .setRequestId(requestId)
                .build()
        return BrokerApi.Command
                .newBuilder()
                .setProducer(producer)
                .setType(BrokerApi.Command.Type.PRODUCER)
                .build()
    }

    fun newSend(sendCommand: CommandSend): Command {
        val res = Command.newBuilder()
                .setType(Command.Type.SEND)
                .setSend(sendCommand)
                .build()
        return res
    }

    fun newSendReceipt(producerId: Long, sequenceId: Long, ledgerId: Long, entryId: Long): Command {
        val sendReceiptBuilder = CommandSendReceipt.newBuilder()
        sendReceiptBuilder.producerId = producerId
        sendReceiptBuilder.sequenceId = sequenceId
        val messageIdBuilder = MessageIdData.newBuilder()
        messageIdBuilder.ledgerId = ledgerId
        messageIdBuilder.entryId = entryId
        val messageId = messageIdBuilder.build()
        sendReceiptBuilder.messageId = messageId
        val sendReceipt = sendReceiptBuilder.build()
        val res = Command.newBuilder()
                .setType(Command.Type.SEND_RECEIPT)
                .setSendReceipt(sendReceipt)
        return res.build()
    }

    fun newSendError(producerId: Long, sequenceId: Long, t: Throwable): Command {
        val sendErrorBuilder = CommandSendError.newBuilder()
        sendErrorBuilder.producerId = producerId
        sendErrorBuilder.sequenceId = sequenceId
        sendErrorBuilder.error = ServerError.PersistenceError
        sendErrorBuilder.message = t.message
        val sendError = sendErrorBuilder.build()
        val res = Command.newBuilder().setType(Command.Type.SEND_ERROR).setSendError(sendError)
        return res.build()
    }

    fun newError(requestId: Long, error: ServerError, message: String): Command {
        val cmdErrorBuilder = CommandError.newBuilder()
        cmdErrorBuilder.requestId = requestId
        cmdErrorBuilder.error = error
        cmdErrorBuilder.message = message
        val cmdError = cmdErrorBuilder.build()
        return Command.newBuilder()
                .setType(Command.Type.ERROR)
                .setError(cmdError)
                .build()
    }

    fun newSubscribe(topic: String, consumerId: Long, requestId: Long, consumerName: String): Command {
        val builder = CommandSubscribe.newBuilder()
        val subscribe = builder.setTopic(topic)
                .setConsumerId(consumerId)
                .setConsumerName(consumerName)
                .setRequestId(requestId)
                .build()
        return Command.newBuilder()
                .setType(Command.Type.SUBSCRIBE)
                .setSubscribe(subscribe)
                .build()
    }

    fun newSuccess(requestId: Long): Command {
        val builder = CommandSuccess.newBuilder()
        builder.requestId = requestId
        val success = builder.build()
        return Command.newBuilder()
                .setType(Command.Type.SUCCESS)
                .setSuccess(success)
                .build()
    }

    fun newPullMessage(topic: String, consumerId: Long, requestId: Long, maxMessage: Int, ledgerId: Long, entryId: Long): Command {
        val builder = CommandPullMessage.newBuilder()
        builder.topic = topic
        builder.consumerId = consumerId
        builder.requestId = requestId
        builder.maxMessage = maxMessage
        builder.messageId = MessageIdData.newBuilder().setLedgerId(ledgerId).setEntryId(entryId).build()
        val pullMessage = builder.build()
        return Command.newBuilder()
                .setType(Command.Type.PULL_MESSAGE)
                .setPullMessage(pullMessage)
                .build()
    }

    fun newMessage(consumerId: Long, ledgerId: Long, entryId: Long, messages: Iterable<CommandSend>): Command {
        val builder = CommandMessage.newBuilder()
        builder.consumerId = consumerId
        builder.nextReadOffset = MessageIdData.newBuilder().setLedgerId(ledgerId).setEntryId(entryId).build()
        builder.addAllMessages(messages)
        val message = builder.build()
        return Command.newBuilder()
                .setType(Command.Type.MESSAGE)
                .setMessage(message)
                .build()
    }

    fun newFetchOffset(topic: String, consumerId: Long, requestId: Long): Command {
        val commandFetchOffset = CommandFetchOffset
                .newBuilder()
                .setTopic(topic)
                .setConsumerId(consumerId)
                .setConsumerName(consumerId.toString())
                .setRequestId(requestId)
                .build()
        return Command.newBuilder()
                .setType(Command.Type.FETCH_CONSUMER_OFFSET)
                .setFetchOffset(commandFetchOffset)
                .build()
    }

    fun newFetchOffsetResponse(topic: String, consumerId: Long, ledgerId: Long, entryId: Long): Command {
        val fetchOffsetResponse = CommandFetchOffsetResponse
                .newBuilder()
                .setTopic(topic)
                .setConsumerId(consumerId)
                .setMessageId(MessageIdData.newBuilder().setLedgerId(ledgerId).setEntryId(entryId))
                .build()
        return Command.newBuilder()
                .setType(Command.Type.FETCH_CONSUMER_OFFSET_RESPONSE)
                .setFetchOffsetResponse(fetchOffsetResponse)
                .build()
    }


    fun newProducerSuccess(producerName: String, requestId: Long): BrokerApi.Command {
        val producerSuccess = BrokerApi.CommandProducerSuccess
                .newBuilder()
                .setProducerName(producerName)
                .setRequestId(requestId)
                .build()
        return Command
                .newBuilder()
                .setProducerSuccess(producerSuccess)
                .setType(Command.Type.PRODUCER_SUCCESS)
                .build()
    }

}