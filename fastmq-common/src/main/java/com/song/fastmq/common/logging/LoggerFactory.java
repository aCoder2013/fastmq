/*
 * Copyright (c) 2009 Red Hat, Inc.
 * -------------------------------------
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * and Apache License v2.0 which accompanies this distribution.
 *
 *     The Eclipse Public License is available at
 *     http://www.eclipse.org/legal/epl-v10.html
 *
 *     The Apache License v2.0 is available at
 *     http://www.opensource.org/licenses/apache2.0.php
 *
 * You may elect to redistribute this code under either of these licenses.
 */

package com.song.fastmq.common.logging;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author <a href="http://tfox.org">Tim Fox</a>
 */
public class LoggerFactory {

    public static final String LOGGER_DELEGATE_FACTORY_CLASS_NAME = "song.fastmq.logger-delegate-factory-class-name";

    private static volatile LogDelegateFactory delegateFactory;

    private static final ConcurrentMap<String, Logger> loggers = new ConcurrentHashMap<>();

    static {
        initialise();
    }

    public static synchronized void initialise() {
        LogDelegateFactory delegateFactory;

        // If a system property is specified then this overrides any delegate factory which is set
        // programmatically - this is primarily of use so we can configure the logger delegate on the client side.
        // call to System.getProperty is wrapped in a try block as it will fail if the client runs in a secured
        // environment
        String className = JULLogDelegateFactory.class.getName();
        try {
            className = System.getProperty(LOGGER_DELEGATE_FACTORY_CLASS_NAME);
        } catch (Exception ignore) {
        }

        if (className != null) {
            ClassLoader loader = Thread.currentThread().getContextClassLoader();
            try {
                Class<?> clz = loader.loadClass(className);
                delegateFactory = (LogDelegateFactory) clz.newInstance();
            } catch (Exception e) {
                throw new IllegalArgumentException("Error instantiating transformer class \"" + className + "\"", e);
            }
        } else if (LoggerFactory.class.getResource("/org/slf4j/LoggerFactory.class") != null) {
            delegateFactory = new SLF4JLogDelegateFactory();
        } else {
            delegateFactory = new JULLogDelegateFactory();
        }

        LoggerFactory.delegateFactory = delegateFactory;
    }

    public static Logger getLogger(final Class<?> clazz) {
        String name = clazz.isAnonymousClass() ?
            clazz.getEnclosingClass().getCanonicalName() :
            clazz.getCanonicalName();
        return getLogger(name);
    }

    public static Logger getLogger(final String name) {
        Logger logger = loggers.get(name);

        if (logger == null) {
            LogDelegate delegate = delegateFactory.createDelegate(name);

            logger = new Logger(delegate);

            Logger oldLogger = loggers.putIfAbsent(name, logger);

            if (oldLogger != null) {
                logger = oldLogger;
            }
        }

        return logger;
    }

    public static void removeLogger(String name) {
        loggers.remove(name);
    }
}
