package io.openmessaging.fastmq.consumer

import io.openmessaging.KeyValue
import io.openmessaging.MessageListener
import io.openmessaging.PushConsumer

/**
 * @author song
 */
class DefaultPushConsumer  : PushConsumer{

    override fun shutdown() {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun suspend() {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun resume() {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun isSuspended(): Boolean {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun properties(): KeyValue {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun startup() {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun attachQueue(queueName: String?, listener: MessageListener?): PushConsumer {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }
}