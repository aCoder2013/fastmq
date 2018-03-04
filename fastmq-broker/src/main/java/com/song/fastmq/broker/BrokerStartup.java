package com.song.fastmq.broker;

import com.song.fastmq.common.logging.Logger;
import com.song.fastmq.common.logging.LoggerFactory;

/**
 * @author song
 */
public class BrokerStartup {

    private static final Logger logger = LoggerFactory.getLogger(BrokerStartup.class);

    public static void main(String[] args) {

        Thread.setDefaultUncaughtExceptionHandler((thread, exception) ->
            logger.error("Uncaught exception in thread {}: {}", thread.getName(), exception.getMessage(), exception));

        BrokerService brokerService = new BrokerService();
        try {
            brokerService.start();
        } catch (Exception e) {
            logger.error("Start broker failed.", e);
            Runtime.getRuntime().exit(1);
        }

        Runtime.getRuntime().addShutdownHook(new Thread(brokerService::close));
        brokerService.waitUntilClosed();
    }

}
