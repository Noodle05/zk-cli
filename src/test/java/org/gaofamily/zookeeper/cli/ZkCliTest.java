package org.gaofamily.zookeeper.cli;

import org.apache.curator.test.TestingServer;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author Wei Gao
 * @since 2/18/17
 */
public class ZkCliTest implements Watcher {
    private static final Logger logger = LoggerFactory.getLogger(ZkCliTest.class);

    private TestingServer zkTestServer;

    @BeforeTest
    public void startZookeeper() throws Exception {
        logger.info("Starting zookeeper");
        zkTestServer = new TestingServer();
        logger.info("Zookeeper connect string: {}", zkTestServer.getConnectString());
    }

    @Test(groups = "unit")
    public void testSingleFile() throws InterruptedException, IOException {
        ZkCli zkCli = new ZkCli();
        logger.debug("Creating temporary file");
        Path tmpFile = Files.createTempFile("Test", "_data");
        ZooKeeper zkClient = new ZooKeeper(zkTestServer.getConnectString(), 1000, this);
        try {
            List<String> lines = new ArrayList<>(10);
            for (int i = 0; i < 10; i++) {
                lines.add(UUID.randomUUID().toString());
            }
            final Lock lock = new ReentrantLock();
            final Condition condition = lock.newCondition();
            lock.lock();
            try {
                Files.write(tmpFile, lines);
                AtomicBoolean synced = new AtomicBoolean(false);
                zkCli.syncFile(tmpFile, "/", zkClient, name -> {
                    logger.debug("Sync single file to {} done.", name);
                    lock.lock();
                    try {
                        synced.compareAndSet(false, true);
                        condition.signal();
                    } finally {
                        lock.unlock();
                    }
                }, exp -> {
                    lock.lock();
                    try {
                        synced.compareAndSet(false, true);
                        condition.signal();
                    } finally {
                        lock.unlock();
                    }
                    Assert.fail("Failed to copy file", exp);
                });
                if (!synced.get()) {
                    condition.await();
                }
            } finally {
                lock.unlock();
            }
        } finally {
            zkClient.close();
            logger.debug("Deleteing temporary file: {}", tmpFile);
            Files.delete(tmpFile);
        }
    }

    @Test(groups = "unit")
    public void testFolder() throws InterruptedException, IOException {
        ZkCli zkCli = new ZkCli();
        Path folder = Paths.get("/Users/wgao/work/trunk/dc_config/dc_config_djsolr/config");
        final Lock lock = new ReentrantLock();
        final Condition condition = lock.newCondition();
        ZooKeeper zkClient = new ZooKeeper(zkTestServer.getConnectString(), 1000, this);
        final Map<String, Throwable> exceptions = new HashMap<>();
        lock.lock();
        try {
            AtomicBoolean synced = new AtomicBoolean(false);
            zkCli.syncFolder(folder, "/", zkClient, name -> {
                logger.debug("Sync folder done.");
                lock.lock();
                try {
                    synced.compareAndSet(false, true);
                    condition.signal();
                } finally {
                    lock.unlock();
                }
            }, exps -> {
                logger.debug("Sync folder done with {} exceptions.", exps.size());
                exceptions.putAll(exps);
                lock.lock();
                try {
                    synced.compareAndSet(false, true);
                    condition.signal();
                } finally {
                    lock.unlock();
                }
            });
            if (!synced.get()) {
                condition.await();
            }
            if (!exceptions.isEmpty()) {
                logger.error("Copy folder failed with {} enties.", exceptions.size());
                exceptions.forEach((np, exp) -> {
                    logger.debug("Path: {} got exception.", np, exp);
                });
                Assert.fail("Copy folder failed.");
            } else {
                logger.info("Sync folder success.");
            }
        } finally {
            zkClient.close();
            lock.unlock();
        }
    }

    @AfterTest
    public void stopZookeeper() throws IOException, InterruptedException {
        logger.info("Stopping zookeeper");
        zkTestServer.stop();
    }

    @Override
    public void process(WatchedEvent event) {
        logger.info("Get watched event: {}", event);
    }
}
