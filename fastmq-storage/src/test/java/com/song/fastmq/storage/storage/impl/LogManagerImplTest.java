package com.song.fastmq.storage.storage.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

import com.song.fastmq.storage.storage.LogReader;
import com.song.fastmq.storage.storage.LogRecord;
import com.song.fastmq.storage.storage.Offset;
import com.song.fastmq.storage.storage.OffsetStorage;
import com.song.fastmq.storage.storage.Version;
import com.song.fastmq.storage.storage.concurrent.AsyncCallbacks;
import com.song.fastmq.storage.storage.concurrent.AsyncCallbacks.CommonCallback;
import com.song.fastmq.storage.storage.config.BookKeeperConfig;
import com.song.fastmq.storage.storage.support.LedgerStorageException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.bookkeeper.client.BookKeeper;
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
 * Created by song on 下午10:02.
 */
@RunWith(MockitoJUnitRunner.class)
public class LogManagerImplTest {

    private static Logger logger = LoggerFactory.getLogger(LogManagerImplTest.class);

    private LogManagerImpl ledgerManager;

    @Mock
    private OffsetStorage offsetStorage;

    @Before
    public void setUp() throws Exception {
        Configurator
            .initialize("FastMQ", Thread.currentThread().getContextClassLoader(), "log4j2.xml");
        CountDownLatch initLatch = new CountDownLatch(1);
        CuratorFramework curatorFramework = CuratorFrameworkFactory
            .newClient("127.0.0.1:2181", new ExponentialBackoffRetry(1000, 3));
        curatorFramework.start();
        AsyncCuratorFramework asyncCuratorFramework = AsyncCuratorFramework.wrap(curatorFramework);
        when(offsetStorage.queryOffset(any())).thenReturn(new Offset());

        ledgerManager = new LogManagerImpl("ledger-manager-read-test-name", new BookKeeperConfig(),
            new BookKeeper("127.0.0.1:2181"), asyncCuratorFramework,
            new LogInfoStorageImpl(asyncCuratorFramework), this.offsetStorage);
        ledgerManager.init(new CommonCallback<Void, LedgerStorageException>() {
            @Override
            public void onCompleted(Void data, Version version) {
                initLatch.countDown();
            }

            @Override
            public void onThrowable(LedgerStorageException throwable) {
                throwable.printStackTrace();
                initLatch.countDown();
            }
        });
        initLatch.await();
    }

    @Test
    public void getName() throws Exception {
        assertEquals("JustATest", ledgerManager.getName());
    }

    @Test(timeout = 3000)
    public void addEntry() throws Throwable {
        class Result {

            Offset first;
            Offset last;
            Throwable throwable;
        }
        Result result = new Result();
        int count = 100;
        AtomicInteger atomicInteger = new AtomicInteger();
        final CountDownLatch downLatch = new CountDownLatch(count);
        for (int i = 0; i < count; i++) {
            ledgerManager.asyncAddEntry(("Hello World" + i).getBytes(),
                new CommonCallback<Offset, LedgerStorageException>() {
                    @Override
                    public void onCompleted(Offset data, Version version) {
                        if (result.first == null) {
                            result.first = data;
                        }
                        result.last = data;
                        downLatch.countDown();
                        atomicInteger.incrementAndGet();
                    }

                    @Override
                    public void onThrowable(LedgerStorageException throwable) {
                        downLatch.countDown();
                        result.throwable = throwable;
                    }
                });
        }
        downLatch.await();
        if (result.throwable != null) {
            throw result.throwable;
        }
        assertEquals(count, atomicInteger.get());
        assertEquals(count, result.last.getEntryId() - result.first.getEntryId() + 1);
    }

    @Test
    public void openCursor() throws Throwable {
        class Result {

            LogReader ledgerCursor;
            Throwable throwable;
        }
        long ledgerId = 0;
        int total = 100;
        for (int i = 0; i < total; i++) {
            Offset offset = this.ledgerManager.addEntry(("entry-" + i).getBytes());
            assertNotNull(offset);
            ledgerId = offset.getLedgerId();
        }
        when(offsetStorage.queryOffset(any())).thenReturn(new Offset(ledgerId, 0));
        Result result = new Result();
        CountDownLatch latch = new CountDownLatch(1);
        ledgerManager.asyncOpenCursor("test-reader", new AsyncCallbacks.OpenCursorCallback() {
            @Override
            public void onComplete(LogReader logReader) {
                result.ledgerCursor = logReader;
                latch.countDown();
            }

            @Override
            public void onThrowable(Throwable throwable) {
                result.throwable = throwable;
                latch.countDown();
            }
        });
        latch.await();
        if (result.throwable != null) {
            throw result.throwable;
        }
        LogReader logReader = result.ledgerCursor;
        CountDownLatch readLatch = new CountDownLatch(1);
        logReader.asyncReadEntries(total, new AsyncCallbacks.ReadEntryCallback() {
            @Override
            public void readEntryComplete(List<LogRecord> entries) {
                assertNotNull(entries);
                assertEquals(total, entries.size());
                entries.forEach(entry -> System.out.println(new String(entry.getData())));
                readLatch.countDown();
            }

            @Override
            public void readEntryFailed(Throwable throwable) {
                throwable.printStackTrace();
                readLatch.countDown();
            }
        });
        readLatch.await();
        logReader.close();
    }

    @After
    public void tearDown() throws Exception {
        if (ledgerManager != null) {
            ledgerManager.close();
        }
    }
}