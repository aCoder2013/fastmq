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

}