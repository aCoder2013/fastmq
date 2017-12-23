package com.song.fastmq.storage.storage.impl;

import com.song.fastmq.storage.storage.BkLedgerStorage;
import com.song.fastmq.storage.storage.LogInfoStorage;
import com.song.fastmq.storage.storage.LogManager;
import com.song.fastmq.storage.storage.Version;
import com.song.fastmq.storage.storage.concurrent.AsyncCallbacks.CommonCallback;
import com.song.fastmq.storage.storage.concurrent.CommonPool;
import com.song.fastmq.storage.storage.config.BookKeeperConfig;
import com.song.fastmq.storage.storage.support.LedgerStorageException;

import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Default implementation of {@link BkLedgerStorage}
 * Created by song on 2017/11/4.
 */
public class BkLedgerStorageImpl implements BkLedgerStorage {

	private static final Logger logger = LoggerFactory.getLogger(BkLedgerStorageImpl.class);

	private volatile Version currentVersion;

	private final BookKeeperConfig bookKeeperConfig;

	private final ZooKeeper zooKeeper;

	private final BookKeeper bookKeeper;

	private final AsyncCuratorFramework asyncCuratorFramework;

	private final LogInfoStorage logInfoStorage;

	private final ConcurrentMap<String, CompletableFuture<LogManager>> ledgers = new ConcurrentHashMap<>();

	public BkLedgerStorageImpl(ClientConfiguration clientConfiguration, BookKeeperConfig config)
		throws Exception {
		bookKeeperConfig = config;
		checkNotNull(clientConfiguration);
		String servers = clientConfiguration.getZkServers();
		checkNotNull(servers);
		final CountDownLatch countDownLatch = new CountDownLatch(1);

		zooKeeper = new ZooKeeper(servers, clientConfiguration.getZkTimeout(), event -> {
			if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
				logger.info("Connected to zookeeper ,connectString = {}", servers);
				countDownLatch.countDown();
			} else {
				logger.error("Failed to connect zookeeper,connectString = {}", servers);
			}
		});
		if (!countDownLatch.await(clientConfiguration.getZkTimeout(), TimeUnit.MILLISECONDS)
			|| zooKeeper.getState() != ZooKeeper.States.CONNECTED) {
			throw new LedgerStorageException(
				"Error connecting to zookeeper server ,connectString = " + servers + ".");
		}

		this.bookKeeper = new BookKeeper(clientConfiguration, zooKeeper);
		RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
		CuratorFramework curatorFramework = CuratorFrameworkFactory.newClient(servers, retryPolicy);
		curatorFramework.start();
		asyncCuratorFramework = AsyncCuratorFramework.wrap(curatorFramework);
		logInfoStorage = new LogInfoStorageImpl(asyncCuratorFramework);
	}

	@Override
	public LogManager open(String name)
		throws LedgerStorageException, InterruptedException {
		Result result = new Result();
		CountDownLatch latch = new CountDownLatch(1);
		asyncOpen(name, new CommonCallback<LogManager, LedgerStorageException>() {
			@Override
			public void onCompleted(LogManager data, Version version) {
				currentVersion = version;
				result.logManager = data;
				latch.countDown();
			}

			@Override
			public void onThrowable(LedgerStorageException throwable) {
				result.exception = throwable;
				latch.countDown();
			}
		});
		latch.await();
		if (result.exception != null) {
			throw result.exception;
		}
		return result.logManager;
	}

	@Override
	public void asyncOpen(String name,
	                      CommonCallback<LogManager, LedgerStorageException> asyncCallback) {
		CompletableFuture<LogManager> completableFuture = ledgers.get(name);
		if (completableFuture != null && completableFuture.isDone()) {
			try {
				LogManager logManager = completableFuture.get();
				asyncCallback.onCompleted(logManager, currentVersion);
			} catch (InterruptedException e) {
				asyncCallback.onThrowable(new LedgerStorageException(e));
			} catch (ExecutionException e) {
				asyncCallback.onThrowable(new LedgerStorageException(e.getCause()));
			}
		}
		CommonPool.executeBlocking(() -> ledgers.computeIfAbsent(name, (mlName) -> {
			CompletableFuture<LogManager> future = new CompletableFuture<>();
			LogManagerImpl ledgerManager = new LogManagerImpl(mlName, bookKeeperConfig,
				// TODO: 2017/12/12 fix null offsetStorage
				bookKeeper, this.asyncCuratorFramework, logInfoStorage, null);
			ledgerManager.init(new CommonCallback<Void, LedgerStorageException>() {
				@Override
				public void onCompleted(Void data, Version version) {
					System.out.println("init回调了!");
					currentVersion = version;
					future.complete(ledgerManager);
				}

				@Override
				public void onThrowable(LedgerStorageException throwable) {
					ledgers.remove(name);
					future.completeExceptionally(throwable);
				}
			});
			return future;
		}).thenAccept(manager -> {
			System.out.println("完成了哦!");
			asyncCallback.onCompleted(manager, new ZkVersion(0));
		})
			.exceptionally(throwable -> {
				asyncCallback.onThrowable(new LedgerStorageException(throwable));
				return null;
			}));
	}

	class Result {

		LogManager logManager;

		LedgerStorageException exception;
	}
}
