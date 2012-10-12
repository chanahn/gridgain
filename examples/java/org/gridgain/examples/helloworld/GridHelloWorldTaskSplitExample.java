package org.gridgain.examples.helloworld;

import org.gridgain.grid.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.typedef.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Demonstrates a simple use of GridGain grid with {@link GridTaskSplitAdapter}.
 * <p>
 * String "Hello Grid Enabled World!" is passed as an argument to
 * {@link Grid#execute(String, Object, GridPredicate[])} method.
 * This method also takes as an argument a task instance, which splits the
 * string into words and wraps each word into a child job, which prints
 * the word to standard output and returns the word length. Those jobs
 * are then distributed among the running nodes. The {@code reduce(...)}
 * method then receives all job results and sums them up. The result
 * of task execution is the number of non-space characters in the
 * sentence that is passed in. All nodes should also print out the words
 * that were processed on them.
 * <p>
 * <h1 class="header">Starting Remote Nodes</h1>
 * To try this example you should (but don't have to) start remote grid instances.
 * You can start as many as you like by executing the following script:
 * <pre class="snippet">{GRIDGAIN_HOME}/bin/ggstart.{bat|sh}</pre>
 * Once remote instances are started, you can execute this example from
 * Eclipse, IntelliJ IDEA, or NetBeans (and any other Java IDE) by simply hitting run
 * button. You will see that all nodes discover each other and
 * some of the nodes will participate in task execution (check node
 * output).
 * <p>
 * <h1 class="header">XML Configuration</h1>
 * If no specific configuration is provided, GridGain will start with
 * all defaults. For information about GridGain default configuration
 * refer to {@link GridFactory} documentation. If you would like to
 * try out different configurations you should pass a path to Spring
 * configuration file as 1st command line argument into this example.
 * The path can be relative to {@code GRIDGAIN_HOME} environment variable.
 * You should also pass the same configuration file to all other
 * grid nodes by executing startup script as follows (you will need
 * to change the actual file name):
 * <pre class="snippet">{GRIDGAIN_HOME}/bin/ggstart.{bat|sh} examples/config/specific-config-file.xml</pre>
 * <p>
 * GridGain examples come with multiple configuration files you can try.
 * All configuration files are located under {@code GRIDGAIN_HOME/examples/config}
 * folder.
 * <p>
 *
 * @author @java.author
 * @version @java.version
 */
public class GridHelloWorldTaskSplitExample {
    /**
     * Execute {@code HelloWorld} example with {@link GridTaskSplitAdapter}.
     *
     * @param args Command line arguments, none required but if provided
     *      first one should point to the Spring XML configuration file. See
     *      {@code "examples/config/"} for configuration file examples.
     * @throws GridException If example execution failed.
     */
    public static void main(String[] args) throws GridException {
        if (args.length == 0)
            G.start();
        else
            G.start(args[0]);

        GridTask<String, Integer> task = new GridTaskSplitAdapter<String, Integer>() {
            /**
             * Splits the received string to words, creates a child job for each word, and sends
             * these jobs to other nodes for processing. Each such job simply prints out the received word.
             *
             * @param gridSize Number of available grid nodes. Note that returned number of
             *      jobs can be less, equal or greater than this grid size.
             * @param arg Task execution argument. Can be {@code null}.
             * @return The list of child jobs.
             */
            @Override protected Collection<? extends GridJob> split(int gridSize, String arg) {
                Collection<GridJob> jobs = new LinkedList<GridJob>();

                for (final String word : arg.split(" ")) {
                    jobs.add(new GridJobAdapterEx() {
                        @Nullable @Override public Object execute() {
                            X.println(">>>");
                            X.println(">>> Printing '" + word + "' on this node from grid job.");
                            X.println(">>>");

                            // Return number of letters in the word.
                            return word.length();
                        }
                    });
                }

                return jobs;
            }

            /** {@inheritDoc} */
            @Nullable @Override public Integer reduce(List<GridJobResult> results) {
                return results.size() - 1 + F.sum(F.<Integer>jobResults(results));
            }
        };

        try {
            GridTaskFuture<Integer> fut = G.grid().execute(task, "Hello Grid Enabled World!");


            // Wait for task completion.
            int phraseLen = fut.get();

            X.println(">>>");
            X.println(">>> Finished executing Grid \"Hello World\" example with custom task.");
            X.println(">>> Total number of characters in the phrase is '" + phraseLen + "'.");
            X.println(">>> You should see print out of 'Hello' on one node and 'World' on another node.");
            X.println(">>> Check all nodes for output (this node is also part of the grid).");
            X.println(">>>");
        }
        finally {
            G.stop(true);
        }
    }
}
