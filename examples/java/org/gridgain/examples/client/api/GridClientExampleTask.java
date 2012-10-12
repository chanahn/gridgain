// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.client.api;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.resources.*;
import org.gridgain.grid.typedef.*;

import java.util.*;

/**
 * Test task that checks key ownership by nodes. This task will produce as many grid jobs as there are
 * nodes in the grid. Each produced job will iterate through all possible keys and peek value from cache.
 * If a value found, corresponding output will be printed. The overall task result is a total count of
 * entries on all nodes.
 */
public class GridClientExampleTask extends GridTaskSplitAdapter<Object, Integer> {
    /** Local grid instance. */
    @GridInstanceResource
    private Grid grid;

    /** {@inheritDoc} */
    @Override protected Collection<? extends GridJob> split(int gridSize, Object arg) throws GridException {
        Collection<GridJob> res = new ArrayList<GridJob>(gridSize);

        for (int i = 0; i < gridSize; i++) {
            res.add(new GridJobAdapterEx() {
                @Override public Integer execute() {
                    GridCache<String, String> cache = grid.cache("partitioned");

                    if (cache == null) {
                        X.println(">>> Partitioned cache is not configured on node: " + grid.localNode().id());

                        return 0;
                    }
                    else {
                        int cnt = 0;

                        for (int i = 0; i < GridClientApiExample.KEYS_CNT; i++) {
                            String key = String.valueOf(i);

                            String val = cache.peek(key);

                            if (val != null) {
                                X.println(">>> Found cache entry [key=" + key + ", val=" + val + ", locNodeId=" +
                                    grid.localNode().id());

                                cnt++;
                            }
                        }

                        return cnt;
                    }
                }
            });
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override public Integer reduce(List<GridJobResult> results) throws GridException {
        int sum = 0;

        for (GridJobResult res : results)
            sum += res.<Integer>getData();

        return sum;
    }
}
