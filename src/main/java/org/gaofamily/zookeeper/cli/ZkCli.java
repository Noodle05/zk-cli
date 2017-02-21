package org.gaofamily.zookeeper.cli;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * @author Wei Gao
 * @since 2/18/17
 */
public class ZkCli {
    private static final Logger logger = LoggerFactory.getLogger(ZkCli.class);

    private static final String PATH_SEPARATOR = "/";

    public void syncFolder(final Path source, final String dest, final ZooKeeper zkClient,
                    Consumer<String> callback, Consumer<Map<String, Throwable>> exceptionHandler) {
        try {
            logger.debug("Syncing folder from: {} to {} on zookeeper: {}", source, dest, zkClient);
            assert Files.isDirectory(source);
            final Path folderName = source.getFileName();
            final String fDest = (dest.endsWith(PATH_SEPARATOR) ? dest : dest + PATH_SEPARATOR) + folderName.toString();
            final byte[] data = new byte[0];
            putData(zkClient, fDest, data, path -> {
                logger.trace("Put data success for path: {}", path);
                try {
                    AtomicInteger counter = new AtomicInteger(0);
                    counter.incrementAndGet();
                    final Map<String, Throwable> exceptions = new HashMap<>();
                    Files.list(source).forEach(file -> {
                        logger.trace("List though souce: {}, get file: {}", source, file);
                        counter.incrementAndGet();
                        if (Files.isRegularFile(file)) {
                            syncFile(file, path, zkClient, name -> countDown(path, callback, exceptionHandler, exceptions, counter), exp -> {
                                exceptions.put(path, exp);
                                countDown(path, callback, exceptionHandler, exceptions, counter);
                            });
                        } else if (Files.isDirectory(file)) {
                            syncFolder(file, path, zkClient, name -> countDown(path, callback, exceptionHandler, exceptions, counter), exps -> {
                                exceptions.putAll(exps);
                                countDown(path, callback, exceptionHandler, exceptions, counter);
                            });
                        }
                    });
                    countDown(path, callback, exceptionHandler, exceptions, counter);
                } catch (Throwable e) {
                    exceptionHandler.accept(Collections.singletonMap(path, e));
                }
            }, exp -> exceptionHandler.accept(Collections.singletonMap(fDest, exp)));
        } catch (Throwable e) {
            exceptionHandler.accept(Collections.singletonMap(dest, e));
        }
    }

    public void syncFile(Path source, String dest, ZooKeeper zkClient,
                  Consumer<String> callback, Consumer<Throwable> exceptionHandler) {
        try {
            logger.debug("Put file from: {} to {} on zookeeper: {}", source, dest, zkClient);
            assert Files.isRegularFile(source);
            Path folderName = source.getFileName();
            final String fDest = (dest.endsWith(PATH_SEPARATOR) ? dest : dest + PATH_SEPARATOR) + folderName.toString();
            final byte[] data = Files.readAllBytes(source);
            putData(zkClient, fDest, data, callback, exceptionHandler);
        } catch (Throwable e) {
            if (exceptionHandler != null) {
                exceptionHandler.accept(e);
            }
        }
    }

    private void countDown(String dest, Consumer<String> callback, Consumer<Map<String, Throwable>> exceptionHandler,
                           Map<String, Throwable> exceptions, AtomicInteger counter) {
        if (counter.decrementAndGet() == 0) {
            logger.trace("All done for {}", dest);
            if (exceptions != null && !exceptions.isEmpty()) {
                logger.trace("But Found {} exception(s)", exceptions.size());
                if (exceptionHandler != null) {
                    exceptionHandler.accept(exceptions);
                }
            } else {
                logger.trace("Success, calling callback");
                if (callback != null) {
                    callback.accept(dest);
                }
            }
        }
    }

    private void putData(ZooKeeper zkClient, final String dest, byte[] data,
                 Consumer<String> callback, Consumer<Throwable> exceptionHandler) {
        try {
            logger.trace("Putting {} bytes to {} on zookeeper: {}", data.length, dest, zkClient);
            zkClient.exists(dest, false, (rc, path, ctx, state) -> {
                logger.trace("Check if {} exists return code: {}", path, rc);
                KeeperException.Code code = KeeperException.Code.get(rc);
                logger.trace("Return code: {} map to: {}", rc, code);
                switch (code) {
                    case OK:
                        logger.trace("Found node on {}, update data.", path);
                        zkClient.setData(path, data, state.getVersion(), (nrc, npath, nctx, nstate) -> {
                            logger.trace("Update data on node {} done, return code: {}", npath, nrc);
                            KeeperException.Code ncode = KeeperException.Code.get(nrc);
                            logger.trace("Result code: {} map to: {}", nrc, ncode);
                            if (ncode.equals(KeeperException.Code.OK)) {
                                if (callback != null) {
                                    logger.trace("Find callback, calling callback with path: {}", npath);
                                    callback.accept(npath);
                                } else {
                                    logger.trace("No callback found.s");
                                }
                            } else {
                                logger.debug("Result code is not OK, throw exception");
                                if (exceptionHandler != null) {
                                    exceptionHandler.accept(KeeperException.create(ncode, npath));
                                } else {
                                    logger.trace("No exception handler found.");
                                }
                            }
                        }, null);
                        break;
                    case NONODE:
                        logger.trace("Didn't find node {}, create one.", path);
                        zkClient.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, (nrc, npath, nctx, nname) -> {
                            logger.trace("Create node {} done, return code: {}", npath, nrc);
                            KeeperException.Code ncode = KeeperException.Code.get(nrc);
                            logger.trace("Result code: {} map to: {}", nrc, ncode);
                            if (ncode.equals(KeeperException.Code.OK)) {
                                if (callback != null) {
                                    logger.trace("Find callback, calling callback with path: {}", npath);
                                    callback.accept(npath);
                                } else {
                                    logger.trace("No callback found.");
                                }
                            } else {
                                logger.debug("Result code is not OK, throw exception");
                                if (exceptionHandler != null) {
                                    exceptionHandler.accept(KeeperException.create(ncode, npath));
                                } else {
                                    logger.trace("No exception handler found.");
                                }
                            }
                        }, null);
                        break;
                    default:
                        logger.debug("Do not recoganize result code, throw exception");
                        if (exceptionHandler != null) {
                            exceptionHandler.accept(KeeperException.create(code, path));
                        } else {
                            logger.trace("No exception handler found.");
                        }
                }
            }, null);
        } catch (Throwable e) {
            if (exceptionHandler != null) {
                exceptionHandler.accept(e);
            } else {
                logger.trace("No exception handler found.");
            }
        }
    }
}
