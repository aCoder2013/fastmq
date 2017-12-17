package com.song.fastmq.client.impl

import com.google.common.base.Preconditions
import com.song.fastmq.client.FastMqClientException
import com.song.fastmq.client.MessageId
import com.song.fastmq.client.Producer
import com.song.fastmq.net.proto.BrokerApi
import com.song.fastmq.storage.common.utils.Utils
import io.netty.buffer.Unpooled
import java.util.concurrent.ExecutionException
import java.util.concurrent.atomic.AtomicReference

/**
 * @author song
 */
class DefaultProducer(val topic: String) : Producer {

    private var state = AtomicReference<State>(State.NONE)

    private var servers: String? = null

    private var clientCnx: ClientCnxClient? = null

    private val cnxPool: RemotingConnectionPool = RemotingConnectionPool()

    override fun start() {
        if (state.compareAndSet(State.NONE, State.CONNECTING)) {
            val connectionFuture = cnxPool.getConnection(Utils.string2SocketAddress(this.servers))
            try {
                this.clientCnx = connectionFuture.get()
            } catch (e: InterruptedException) {
                throw FastMqClientException(e)
            } catch(e: ExecutionException) {
                throw FastMqClientException(e.cause)
            }
        } else {
            throw FastMqClientException("The Producer [$topic] is already connecting to broker!")
        }
    }

    override fun setServer(servers: String) {
        Preconditions.checkArgument(!servers.isNullOrBlank())
        this.servers = servers
    }

    override fun send(message: ByteArray): MessageId {
        val producer = BrokerApi.CommandProducer.newBuilder().setProducerId(1L).setProducerName("Hello").setTopic(topic).setRequestId(1L)
                .build()
        val command = BrokerApi.Command.newBuilder().setProducer(producer).setType(BrokerApi.Command.Type.PRODUCER).build()
        val toByteArray = command.toByteArray()
        val byteBuf = Unpooled.buffer(4 + toByteArray.size)
        val size = toByteArray.size
        byteBuf.writeInt(size)
        byteBuf.writeBytes(toByteArray)
        clientCnx?.ctx?.writeAndFlush(byteBuf)?.sync()
        return MessageId(0L, 0L)
    }

    override fun close() {
        this.state.set(State.NONE)
    }

    enum class State {
        NONE,
        CONNECTING,
        CONNECTED
    }
}