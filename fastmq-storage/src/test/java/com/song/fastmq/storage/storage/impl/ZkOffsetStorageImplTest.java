package com.song.fastmq.storage.storage.impl;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

import com.song.fastmq.storage.storage.LogInfoStorage;
import com.song.fastmq.storage.storage.LogReaderInfo;
import com.song.fastmq.storage.storage.Offset;
import com.song.fastmq.storage.storage.OffsetStorage;
import com.song.fastmq.storage.storage.metadata.Log;
import com.song.fastmq.storage.storage.metadata.LogSegment;
import java.util.Collections;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author song
 */
@RunWith(MockitoJUnitRunner.class)
public class ZkOffsetStorageImplTest {

    private static Logger logger = LoggerFactory.getLogger(ZkOffsetStorageImplTest.class);

    @Mock
    private LogInfoStorage logInfoStorage;

    private OffsetStorage offsetStorage;

    private AsyncCuratorFramework asyncCuratorFramework;

    private int ledgerId = 0;

    @Before
    public void setUp() throws Exception {
        Configurator
            .initialize("FastMQ", Thread.currentThread().getContextClassLoader(), "log4j2.xml");
        Log log = new Log();
        LogSegment logSegment = new LogSegment();
        logSegment.setLedgerId(ledgerId);
        logSegment.setTimestamp(System.currentTimeMillis());
        log.setSegments(Collections.singletonList(logSegment));
        when(logInfoStorage.getLogInfo(any())).thenReturn(log);

        CuratorFramework curatorFramework = CuratorFrameworkFactory
            .newClient("127.0.0.1:2181", new ExponentialBackoffRetry(1000, 3));
        curatorFramework.start();
        asyncCuratorFramework = AsyncCuratorFramework.wrap(curatorFramework);
        offsetStorage = new ZkOffsetStorageImpl(logInfoStorage, asyncCuratorFramework);
    }

    @Test
    public void queryOffset() throws Exception {
        LogReaderInfo logReaderInfo = new LogReaderInfo();
        logReaderInfo.setTopic("test");
        logReaderInfo.setConsumer("test-consumer");
        Offset offset = this.offsetStorage.queryOffset(logReaderInfo);
        assertEquals(ledgerId, offset.getLedgerId());
        assertEquals(0, offset.getEntryId());
    }

    @After
    public void tearDown() throws Exception {
        LogReaderInfo logReaderInfo = new LogReaderInfo();
        logReaderInfo.setTopic("test");
        logReaderInfo.setConsumer("test-consumer");
        this.offsetStorage.removeOffset(logReaderInfo);
    }
}