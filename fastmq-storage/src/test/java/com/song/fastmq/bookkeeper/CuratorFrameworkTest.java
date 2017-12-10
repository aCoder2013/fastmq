package com.song.fastmq.bookkeeper;

import com.song.fastmq.common.utils.JsonUtils;
import java.util.EnumSet;
import java.util.concurrent.CountDownLatch;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.curator.x.async.api.CreateOption;
import org.junit.Before;
import org.junit.Test;

/**
 * @author song
 */
public class CuratorFrameworkTest {

    private AsyncCuratorFramework curatorFramework;

    @Before
    public void setUp() throws Exception {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        CuratorFramework curatorFramework = CuratorFrameworkFactory.newClient("127.0.0.1:2181", retryPolicy);
        curatorFramework.start();
        this.curatorFramework = AsyncCuratorFramework.wrap(curatorFramework);
    }

    @Test
    public void create() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        this.curatorFramework.create().withOptions(EnumSet.of(CreateOption.createParentsIfNeeded)).forPath("/test/hello", "Hello World".getBytes()).whenComplete((s, throwable) -> {
            if (throwable != null) {
                throwable.printStackTrace();
                latch.countDown();
            }
            System.out.println(s);
            latch.countDown();
        });
        latch.await();
    }

    @Test
    public void get() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        this.curatorFramework.getData().forPath("/fastmq/ledger-cursors/HelloWorldTest/test-reader").whenComplete((bytes, throwable) -> {
            if (throwable != null) {
                throwable.printStackTrace();
                latch.countDown();
                return;
            }
            System.out.println(new String(bytes));
            latch.countDown();
        });
        latch.await();
    }

    @Test
    public void checkExist() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        this.curatorFramework.checkExists().forPath("/fastmq/ledger-cursors/HelloWorldTest/test-reader").whenComplete((stat, throwable) -> {
            if (throwable != null) {
                throwable.printStackTrace();
                latch.countDown();
                return;
            }
            System.out.println(JsonUtils.toJsonQuietly(stat));
            latch.countDown();
        });
        latch.await();
    }
}
