package com.song.fastmq.net.proto

/**
 * @author song
 */
object Commands {

    fun newSend(sendCommand: BrokerApi.CommandSend): BrokerApi.Command {
        var res = BrokerApi.Command.newBuilder()
                .setType(BrokerApi.Command.Type.SEND)
                .setSend(sendCommand)
                .build()
        return res
    }

    fun newSendReceipt(producerId: Long, sequenceId: Long, ledgerId: Long, entryId: Long): BrokerApi.Command {
        val sendReceiptBuilder = BrokerApi.CommandSendReceipt.newBuilder()
        sendReceiptBuilder.producerId = producerId
        sendReceiptBuilder.sequenceId = sequenceId
        val messageIdBuilder = BrokerApi.MessageIdData.newBuilder()
        messageIdBuilder.ledgerId = ledgerId
        messageIdBuilder.entryId = entryId
        val messageId = messageIdBuilder.build()
        sendReceiptBuilder.messageId = messageId
        val sendReceipt = sendReceiptBuilder.build()
        val res = BrokerApi.Command.newBuilder()
                .setType(BrokerApi.Command.Type.SEND_RECEIPT)
                .setSendReceipt(sendReceipt)
        return res.build()
    }

    fun newSendError(producerId: Long, sequenceId: Long, t: Throwable): BrokerApi.Command {
        val sendErrorBuilder = BrokerApi.CommandSendError.newBuilder()
        sendErrorBuilder.producerId = producerId
        sendErrorBuilder.sequenceId = sequenceId
        sendErrorBuilder.error = BrokerApi.ServerError.PersistenceError
        sendErrorBuilder.message = t.message
        val sendError = sendErrorBuilder.build()
        val res = BrokerApi.Command.newBuilder().setType(BrokerApi.Command.Type.SEND_ERROR).setSendError(sendError)
        return res.build()
    }

    fun newError(requestId: Long, error: BrokerApi.ServerError, message: String): BrokerApi.Command {
        val cmdErrorBuilder = BrokerApi.CommandError.newBuilder()
        cmdErrorBuilder.requestId = requestId
        cmdErrorBuilder.error = error
        cmdErrorBuilder.message = message
        val cmdError = cmdErrorBuilder.build()
        return BrokerApi.Command.newBuilder()
                .setType(BrokerApi.Command.Type.ERROR)
                .setError(cmdError)
                .build()
    }

    fun newSubscribe(topic: String, consumerId: Long, requestId: Long, consumerName: String): BrokerApi.Command {
        val builder = BrokerApi.CommandSubscribe.newBuilder()
        val subscribe = builder.setTopic(topic)
                .setConsumerId(consumerId)
                .setConsumerName(consumerName)
                .setRequestId(requestId)
                .build()
        return BrokerApi.Command.newBuilder()
                .setType(BrokerApi.Command.Type.SUBSCRIBE)
                .setSubscribe(subscribe)
                .build()
    }

    fun newSuccess(requestId: Long): BrokerApi.Command {
        val builder = BrokerApi.CommandSuccess.newBuilder()
        builder.requestId = requestId
        val success = builder.build()
        return BrokerApi.Command.newBuilder()
                .setType(BrokerApi.Command.Type.SUCCESS)
                .setSuccess(success)
                .build()
    }

    fun newPullMessage(topic: String, consumerId: Long, requestId: Long, maxMessage: Int): BrokerApi.Command {
        val builder = BrokerApi.CommandPullMessage.newBuilder()
        builder.topic = topic
        builder.consumerId = consumerId
        builder.requestId = requestId
        builder.maxMessage = maxMessage
        val pullMessage = builder.build()
        return BrokerApi.Command.newBuilder()
                .setType(BrokerApi.Command.Type.PULL_MESSAGE)
                .setPullMessage(pullMessage)
                .build()
    }

    fun newMessage(consumerId: Long, messages: Iterable<BrokerApi.CommandSend>): BrokerApi.Command {
        val builder = BrokerApi.CommandMessage.newBuilder()
        builder.consumerId = consumerId
        builder.addAllMessages(messages)
        val message = builder.build()
        return BrokerApi.Command.newBuilder()
                .setType(BrokerApi.Command.Type.MESSAGE)
                .setMessage(message)
                .build()
    }


}