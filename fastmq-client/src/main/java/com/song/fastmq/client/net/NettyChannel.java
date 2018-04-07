package com.song.fastmq.client.net;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;

/**
 * @author song
 */
public class NettyChannel {

    private final ChannelFuture channelFuture;

    public NettyChannel(ChannelFuture channelFuture) {
        this.channelFuture = channelFuture;
    }

    public boolean isOK() {
        return this.channelFuture.channel() != null && this.channelFuture.channel().isActive();
    }

    public boolean isWriteable() {
        return this.channelFuture.channel().isWritable();
    }

    public Channel getChannel() {
        return this.channelFuture.channel();
    }

    public ChannelFuture getChannelFuture() {
        return channelFuture;
    }

}
