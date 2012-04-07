// Copyright (C) GridGain Systems Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.client;

import net.sf.json.*;
import org.gridgain.grid.*;
import org.gridgain.grid.typedef.*;
import org.gridgain.grid.util.*;

import java.util.*;

/**
 * Starts up grid node (server) with pre-defined ports and tasks to test client-server interactions.
 * <p>
 * Note that different nodes cannot share the same port for rest services. If you want
 * to start more than one node on the same physical machine you must provide different
 * configurations for each node. Otherwise, this example would not work.
 * <p>
 * After this example has been started you can use pre-defined endpoints and task names in your
 * client-server interactions to work with the node over un-secure protocols (binary or http).
 * <p>
 * Available end-points:
 * <ul>
 *     <li>127.0.0.1:10080 - TCP unsecured endpoint.</li>
 *     <li>127.0.0.1:11080 - HTTP unsecured endpoint.</li>
 * </ul>
 * <p>
 * Required credentials for remote client authentication: "s3cret".
 *
 * @author 2012 Copyright (C) GridGain Systems
 * @version 4.0.1c.07042012
 */
public class GridServerNodeStartup {
    /**
     * Starts up two nodes with specified cache configuration on pre-defined endpoints.
     *
     * @param args Command line arguments, none required.
     * @throws Exception In case of any exception.
     */
    public static void main(String[] args) throws Exception {
        if (!GridUtils.isEnterprise()) {
            X.println(">>> This example is available in enterprise edition only.\n" +
                ">>> \n" +
                ">>> To start this example with community edition you shoud disable\n" +
                ">>> authentication and secure session SPIs in the configuration files.");

            return;
        }

        try {
            G.start("examples/config/spring-server-node.xml");

            X.println(">>> Press 'Ctrl+C' to stop the process...");

            Thread.sleep(Long.MAX_VALUE);
        } finally {
            G.stopAll(true);
        }
    }

    /**
     * Test task summarizes length of all strings in the arguments list.
     * <p>
     * The argument of the task is a collection of objects to calculate string length summ of.
     */
    protected static class TestTask extends GridTaskSplitAdapter<List<Object>, Integer> {
        /** {@inheritDoc} */
        @Override protected Collection<? extends GridJob> split(int gridSize, List<Object> list)
            throws GridException {
            Collection<GridJobAdapterEx> jobs = new ArrayList<GridJobAdapterEx>();

            if (list != null)
                for (final Object val : list)
                    jobs.add(new GridJobAdapterEx() {
                        @Override public Object execute() {
                            try {
                                Thread.sleep(5);
                            }
                            catch (InterruptedException ignored) {
                                Thread.currentThread().interrupt();
                            }

                            return val == null ? 0 : val.toString().length();
                        }
                    });

            return jobs;
        }

        /** {@inheritDoc} */
        @Override public Integer reduce(List<GridJobResult> results) throws GridException {
            int sum = 0;

            for (GridJobResult res : results)
                sum += res.<Integer>getData();

            return sum;
        }
    }

    /**
     * Test task summarizes length of all strings in the arguments list.
     * <p>
     * The argument of the task is JSON-serialized array of objects to calculate string length summ of.
     */
    protected static class HttpTestTask extends GridTaskSplitAdapter<String, Integer> {
        /** Task delegate. */
        private final TestTask delegate = new TestTask();

        /** {@inheritDoc} */
        @Override protected Collection<? extends GridJob> split(int gridSize, String arg) throws GridException {
            JSON json = JSONSerializer.toJSON(arg);

            List list = json.isArray() ? JSONArray.toList((JSONArray) json, String.class, new JsonConfig()) : null;

            //noinspection unchecked
            return delegate.split(gridSize, list);
        }

        /** {@inheritDoc} */
        @Override public Integer reduce(List<GridJobResult> results) throws GridException {
            return delegate.reduce(results);
        }
    }
}
