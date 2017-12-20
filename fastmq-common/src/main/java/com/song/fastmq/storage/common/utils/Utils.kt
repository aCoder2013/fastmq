package com.song.fastmq.storage.common.utils

import org.slf4j.LoggerFactory
import java.lang.management.ManagementFactory
import java.net.Inet6Address
import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.NetworkInterface
import java.util.*

/**
 * Created by song on 2017/11/5.
 */
object Utils {

    private val logger = LoggerFactory.getLogger(Utils::class.java)

    val AVAILABLE_PROCESSORS = Runtime.getRuntime().availableProcessors()

    fun getLocalAddress(): String? {
        try {
            // Traversal Network interface to get the first non-loopback and non-private address
            val enumeration = NetworkInterface.getNetworkInterfaces()
            val ipv4Result = ArrayList<String>()
            val ipv6Result = ArrayList<String>()
            while (enumeration.hasMoreElements()) {
                val networkInterface = enumeration.nextElement()
                val en = networkInterface.inetAddresses
                while (en.hasMoreElements()) {
                    val address = en.nextElement()
                    if (!address.isLoopbackAddress) {
                        if (address is Inet6Address) {
                            ipv6Result.add(normalizeHostAddress(address))
                        } else {
                            ipv4Result.add(normalizeHostAddress(address))
                        }
                    }
                }
            }

            // prefer ipv4
            if (!ipv4Result.isEmpty()) {

                return ipv4Result.firstOrNull { !it.startsWith("127.0") && !it.startsWith("192.168") }
                        ?: ipv4Result[ipv4Result.size - 1]
            } else if (!ipv6Result.isEmpty()) {
                return ipv6Result[0]
            }
            //If failed to find,fall back to localhost
            val localHost = InetAddress.getLocalHost()
            return normalizeHostAddress(localHost)
        } catch (e: Exception) {
            logger.error("Failed to obtain local address", e)
        }

        return null
    }

    fun normalizeHostAddress(localHost: InetAddress): String {
        if (localHost is Inet6Address) {
            return "[" + localHost.getHostAddress() + "]"
        } else {
            return localHost.hostAddress
        }
    }


    fun string2SocketAddress(addr: String): InetSocketAddress {
        val s = addr.split(":".toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray()
        return InetSocketAddress(s[0], Integer.parseInt(s[1]))
    }

    fun getPid(): Int {
        val runtime = ManagementFactory.getRuntimeMXBean()
        val name = runtime.name // format: "pid@hostname"
        try {
            return Integer.parseInt(name.substring(0, name.indexOf('@')))
        } catch (e: Exception) {
            logger.error("Get pid failed", e)
            return -1
        }
    }
}
