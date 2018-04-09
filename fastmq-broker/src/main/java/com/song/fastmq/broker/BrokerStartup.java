package com.song.fastmq.broker;

import com.song.fastmq.common.logging.Logger;
import com.song.fastmq.common.logging.LoggerFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.config.Configurator;

/**
 * @author song
 */
public class BrokerStartup {

    private static final Logger logger = LoggerFactory.getLogger(BrokerStartup.class);

    public static void main(String[] args) {
        Configurator.initialize("FastMQ", Thread.currentThread().getContextClassLoader(), "log4j2.xml");
        Thread.setDefaultUncaughtExceptionHandler((thread, exception) -> logger.error("Uncaught exception in thread", exception));

        BrokerService brokerService = new BrokerService();
        try {
            brokerService.start();
        } catch (Exception e) {
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }

        Runtime.getRuntime().addShutdownHook(new Thread(brokerService::close));
        Runtime.getRuntime().addShutdownHook(new Thread(LogManager::shutdown));
        brokerService.waitUntilClosed();
    }

}
