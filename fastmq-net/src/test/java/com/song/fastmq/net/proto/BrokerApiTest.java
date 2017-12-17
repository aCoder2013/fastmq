package com.song.fastmq.net.proto;

import com.song.fastmq.net.proto.BrokerApi.Command;
import com.song.fastmq.net.proto.BrokerApi.Command.Builder;
import com.song.fastmq.net.proto.BrokerApi.Command.Type;
import com.song.fastmq.net.proto.BrokerApi.CommandProducer;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * @author song
 */
public class BrokerApiTest {

    @Before
    public void setUp() throws Exception {
        Configurator
            .initialize("FastMQ", Thread.currentThread().getContextClassLoader(), "log4j2.xml");
    }

    @Test
    public void parse() throws Exception {
        CommandProducer commandProducer = CommandProducer.newBuilder()
            .setProducerId(0L)
            .setTopic("HelloWorld").build();

        Command command = Command.newBuilder().setProducer(commandProducer).setType(Type.PRODUCER)
            .build();
        byte[] bytes = command.toByteArray();
        Builder builder = Command.newBuilder().mergeFrom(bytes);
        Assert.assertEquals(command, builder.build());
    }
}