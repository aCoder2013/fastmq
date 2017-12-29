package com.song.fastmq.storage.storage

import com.song.fastmq.storage.storage.concurrent.AsyncCallbacks
import com.song.fastmq.storage.storage.config.BookKeeperConfig
import com.song.fastmq.storage.storage.impl.LogManagerFactoryImpl
import com.song.fastmq.storage.storage.support.LedgerStorageException
import org.apache.bookkeeper.conf.ClientConfiguration
import org.junit.Test

/**
 * @author song
 */
class LogManagerFactoryTest {

    @Test
    @Throws(Exception::class)
    fun open() {
        val clientConfiguration = ClientConfiguration()
        clientConfiguration.zkServers = "127.0.0.1:2181"
        val bkConfig = BookKeeperConfig()
        val bkLedgerStorage = LogManagerFactoryImpl(clientConfiguration, bkConfig)
        bkLedgerStorage.asyncOpen("just-a-test", object : AsyncCallbacks.CommonCallback<LogManager, LedgerStorageException> {
            override fun onCompleted(data: LogManager?, version: Version?) {
                println("成功了!")
            }

            override fun onThrowable(throwable: LedgerStorageException) {
                println("失败了")
                throwable.printStackTrace()
            }

        })
        Thread.sleep(10000000)
    }
}