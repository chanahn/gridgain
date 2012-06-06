// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.junit.junit4;

import org.gridgain.grid.test.junit4.*;
import org.junit.runner.*;
import org.junit.runners.Suite.*;

/**
 * This suite is nested within {@link GridJunit4ExampleSuite} suite. By wrapping
 * {@link TestC} into a local suite we guarantee that this test will always execute
 * on local node, however still in parallel with other tests.
 *
 * @author @java.author
 * @version @java.version
 */
@RunWith(GridJunit4LocalSuite.class) // Specify local suite to run tests.
@SuiteClasses(TestC.class)
public class GridJunit4ExampleNestedLocalSuite {
    // No-op.
}
