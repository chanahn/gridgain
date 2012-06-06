// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.dataload;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.gridgain.examples.cache.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.resources.*;
import org.gridgain.grid.typedef.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import static java.util.concurrent.TimeUnit.*;

/**
 * This example demonstrates HDFS data loader.
 * We first populate HDFS by putting {@link #FILES_CNT} files into {@link #HDFS_DIR},
 * and then we start node with configured HDFS loader to load data from HDFS to
 * distributed cache.
 * <p>
 * This example does not require distributed HDFS setup and can run with local file system.
 * If HDFS cluster is available, add HDFS config files to example classpath.
 * <p>
 * When starting remote nodes, make sure to use the same configuration file as follows:
 * <pre>
 *     GRIDGAIN_HOME/bin/ggstart.sh examples/config/spring-cache.xml
 * </pre>
 *
 * @author @java.author
 * @version @java.version
 */
public class GridCacheHdfsDataLoaderExample {
    /** GridGain home. */
    public static final String GG_HOME = X.getSystemOrEnv(GridSystemProperties.GG_HOME);

    /** Directory to process. */
    private static final String HDFS_DIR = "gg-cache";

    /** Local directory to process (in case HDFS is not available). */
    private static final String LOCAL_DIR = "work/hdfs/gg-cache";

    /** Files count. */
    private static final int FILES_CNT = 20;

    /** Key-value pairs per file count. */
    private static final int ITEMS_CNT = 5;

    /** Workers pool size. */
    private static final int HDFS_WORKERS_CNT = 3;

    /** File name prefix. */
    private static final String FILE_NAME_PREF = "cache-data-file";

    /** File name index format. */
    private static final String FILE_IDX_FMT = "%05d";

    /** Cache name. */
    private static final String CACHE_NAME = "partitioned";
    //private static final String CACHE_NAME = "replicated";
    //private static final String CACHE_NAME = "local";

    /** Thread pool for reading different HDFS files in parallel. */
    private static ExecutorService exec;

    /**
     * Starts HDFS data loading example.
     *
     * @param args Arguments (none required).
     * @throws Exception If failed.
     */
    public static void main(String[] args) throws Exception {
        if (GG_HOME == null || GG_HOME.isEmpty()) {
            X.error(">>>", ">>> GRIDGAIN_HOME environment or system variable should be set in order " +
                "to run this example.", ">>>");

            return;
        }

        // Populate HDFS in order to create some test data to load from.
        populateHdfs();

        try {
            Grid grid = G.start("examples/config/spring-cache.xml");

            loadData(grid);

            Thread.sleep(5000);

            // Ack default cache key set size on all nodes
            // to make sure loading succeeded.
            grid.run(
                GridClosureCallMode.BROADCAST,
                new Runnable() {
                    @GridInstanceResource
                    private Grid g;

                    @Override public void run() {
                        GridCache<Object, Object> cache = g.cache(CACHE_NAME);

                        if (cache != null)
                            X.println(">>>", ">>> Cache primary key set size [nodeId=" + g.localNode().id() +
                                ", size=" + cache.primaryKeySet().size() + ']', ">>>");
                        else
                            X.println(">>>", ">>> Cache is not configured on local node [nodeId=" + g.localNode().id() +
                                ", cacheName=" + CACHE_NAME + ']', ">>>");
                    }
                }
            );
        }
        finally {
            G.stopAll(true);

            cleanupHdfs();
        }
    }

    /**
     * Loads data from HDFS by recursively descending into subdirectories and
     * reading files. Each individual file is loaded by {@link HdfsWorker}
     * within a separate thread for better parallelism.
     *
     * @param grid Grid to load data with.
     * @throws Exception If failed.
     */
    private static void loadData(Grid grid) throws Exception {
        // Get data loader - it will be used to load data onto data grid.
        final GridDataLoader<UUID, Person> ldr = grid.dataLoader(CACHE_NAME);

        // Configure loader.
        ldr.perNodeBufferSize(2);
        ldr.perNodeParallelLoadOperations(2);

        X.println(">>>", ">>> Using data loader for cache: " + ldr.cacheName(), ">>>");

        FileSystem fs = null;

        try {
            exec = Executors.newFixedThreadPool(HDFS_WORKERS_CNT);

            Configuration cfg = new Configuration();

            fs = FileSystem.get(cfg);

            Path path = path(fs);

            if (!fs.exists(path))
                // Directory does not exist.
                return;

            Queue<Future<?>> futs = new LinkedList<Future<?>>();

            // Walk down the directory and assign workers to load individual files.
            walk(fs, path, ldr, futs);

            // Wait for all HDFS reader threads to finish.
            for (Future<?> f = futs.poll(); f != null; f = futs.poll())
                f.get();

            // Close loader without cancellation to complete data loading session.
            ldr.close(false);
        }
        finally {
            closeQuiet(fs);

            exec.shutdownNow();

            exec.awaitTermination(2000, MILLISECONDS);
        }
    }

    /**
     * Walks through the HDFS directory and passes found files to given closure.
     *
     * @param fs File system object.
     * @param path HDFS directory.
     * @param ldr Data loader to load data onto grid.
     * @param futs Futures for submitted tasks.
     * @throws IOException If failed.
     */
    private static void walk(FileSystem fs, Path path, GridDataLoader<UUID, Person> ldr, Queue<Future<?>> futs)
        throws IOException {
        assert fs != null;
        assert path != null;

        for (FileStatus s : fs.listStatus(path)) {
            if (s.isDir())
                walk(fs, s.getPath(), ldr, futs);
            else
                futs.add(exec.submit(new HdfsWorker(fs, s.getPath(), ldr)));
        }
    }

    /**
     * Populates HDFS with example data just so we can then read it and
     * load it onto data grid using {@link GridDataLoader} instance.
     *
     * @throws Exception If failed.
     */
    private static void populateHdfs() throws Exception {
        Configuration cfg = new Configuration();

        FileSystem fs = null;

        try {
            fs = FileSystem.get(cfg);

            X.println(">>>", ">>> Pre-populating HDFS.", ">>>");

            Path dirPath = path(fs);

            // Always re-create data files.
            if (fs.exists(dirPath)) {
                X.println(">>> Removing old data.");

                fs.delete(dirPath, true);
            }
            else
                X.println(">>> No old data to delete.");

            for (int i = 0; i < FILES_CNT; i++) {
                Path filePath = new Path(dirPath, FILE_NAME_PREF + String.format(FILE_IDX_FMT, i));

                assert !fs.exists(filePath);

                ObjectOutputStream out = null;

                try {
                    X.println(">>> Creating new file: " + filePath);

                    out = new ObjectOutputStream(fs.create(filePath));

                    for (int j = 0; j < ITEMS_CNT; j++) {
                        UUID id = UUID.randomUUID();

                        Person p = new Person(id);

                        p.setFirstName("FirstName" + (ITEMS_CNT * i + j));
                        p.setLastName("LastName" + (ITEMS_CNT * i + j));

                        // Write key.
                        out.writeObject(id);

                        // Write value.
                        out.writeObject(p);
                    }
                }
                finally {
                    closeQuiet(out);
                }
            }
        }
        finally {
            closeQuiet(fs);
        }
    }

    /**
     * Cleans up HDFS.
     *
     * @throws Exception If failed.
     */
    private static void cleanupHdfs() throws Exception {
        Configuration cfg = new Configuration();

        FileSystem fs = null;

        try {
            fs = FileSystem.get(cfg);

            X.println(">>>", ">>> Cleaning up HDFS.", ">>>");

            Path dirPath = path(fs);

            fs.delete(dirPath, true);
        }
        finally {
            closeQuiet(fs);
        }
    }

    /**
     * Resolves path to process.
     * @param fs File system.
     * @return Path.
     */
    private static Path path(FileSystem fs) {
        assert fs != null;

        return fs instanceof LocalFileSystem ? new Path(GG_HOME, LOCAL_DIR) : new Path(HDFS_DIR);
    }

    /**
     * Quietly closes a resource.
     *
     * @param rsrc Resource to close.
     */
    private static void closeQuiet(@Nullable Closeable rsrc) {
        if (rsrc != null) {
            try {
                rsrc.close();
            }
            catch (IOException ignore) {
                // No-op.
            }
        }
    }

    /**
     * Worker that processes HDFS path. It iterates over all objects found
     * in given path and adds them to {@link GridDataLoader} to be loaded
     * onto data grid.
     */
    private static class HdfsWorker implements Callable<Object> {
        /** Path to process. */
        private Path path;

        /** Loader. */
        private GridDataLoader<UUID, Person> ldr;

        /** HDFS. */
        private FileSystem fs;

        /**
         * @param fs File system.
         * @param path Path.
         * @param ldr Loader.
         */
        private HdfsWorker(FileSystem fs, Path path, GridDataLoader<UUID, Person> ldr) {
            assert path != null;
            assert ldr != null;

            this.fs = fs;
            this.path = path;
            this.ldr = ldr;
        }

        /** {@inheritDoc} */
        @Nullable @Override public Object call() throws Exception {
            ObjectInputStream in = null;

            try {
                in = new ObjectInputStream(fs.open(path));

                while (true) {
                    try {
                        UUID personId = (UUID)in.readObject();
                        Person person = (Person)in.readObject();

                        // Add data to load onto data grid.
                        ldr.addData(personId, person);
                    }
                    catch (EOFException ignored) {
                        X.println("HDFS path has been processed: " + path);

                        break;
                    }
                    catch (ClassNotFoundException e) {
                        X.println(">>> Class not found: " + e);
                    }
                }
            }
            catch (Exception e) {
                X.error("Failed to process path: " + path, e);

                throw e;
            }
            finally {
                closeQuiet(in);
            }

            return null;
        }
    }
}
