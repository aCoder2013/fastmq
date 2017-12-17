package com.song.fastmq.storage.common.utils;

import java.net.InetSocketAddress;

/**
 * Created by song on 2017/11/5.
 */
public class Utils {

    public static final int AVAILABLE_PROCESSORS = Runtime.getRuntime().availableProcessors();

    public static InetSocketAddress string2SocketAddress(final String addr) {
        String[] s = addr.split(":");
        return new InetSocketAddress(s[0], Integer.parseInt(s[1]));
    }
}
