package org.gridgain.examples.session;

import org.gridgain.grid.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.resources.*;
import org.gridgain.grid.typedef.*;

import java.util.*;

/**
 * Demonstrates a simple use of GridGain grid with the use
 * of task session.
 * <p>
 * String "Hello World" is passed as an argument to
 * {@link Grid#execute(GridTask, Object, GridPredicate[])} along with a
 * {@link GridTask} instance, that is distributed amongst the nodes. Task
 * on each of the participating nodes will do the following:
 * <ol>
 * <li>
 *      Set it's argument as a session attribute.
 * </li>
 * <li>
 *      Wait for other jobs to set their arguments as a session attributes.
 * </li>
 * <li>
 *      Concatenate all session attributes (which in this case are all job arguments)
 *      into one string and print it out.
 * </li>
 * </ol>
 * One of the potential outcomes could look as following:
 * <pre class="snippet">
 * All session attributes [ Hello World ]
 * >>>
 * >>> Printing 'World' on this node.
 * >>>
 * </pre>
 * <p>
 * Grid task {@link GridTaskSplitAdapter} handles actual splitting
 * into sub-jobs, remote execution, and result reduction (see {@link GridTask}).
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
public class GridSessionExample {
    /**
     * Method, that simply prints out the argument passed in and returns it's length.
     *
     * @param phrase Phrase string to print.
     * @return Number of characters in the phrase.
     */
    public static int sayIt(CharSequence phrase) {
        // Simply print out the argument.
        X.println(">>>");
        X.println(">>> Printing '" + phrase + "' on this node.");
        X.println(">>>");

        return phrase.length();
    }

    /**
     * Execute {@code HelloWorld} example with session attributes.
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
            /** Grid task session is injected here. */
            @GridTaskSessionResource
            private GridTaskSession ses;

            /**
             * Splits the passed in phrase into words and creates a job for every
             * word. Every job will print out full phrase, the word and return
             * number of letters in that word.
             *
             * @param gridSize Number of nodes in the grid.
             * @param arg Task execution argument.
             * @return Created grid jobs for remote execution.
             */
            @Override protected Collection<? extends GridJob> split(int gridSize, String arg) {
                String[] words = arg.split(" ");

                Collection<GridJobAdapterEx> jobs = new ArrayList<GridJobAdapterEx>(words.length);

                for (String word : words) {
                    jobs.add(new GridJobAdapterEx(word) {
                        /** Job context will be injected. */
                        @GridJobContextResource
                        private GridJobContext jobCtx;

                        /**
                         * Prints out all session attributes concatenated into string
                         * and runs {@link GridSessionExample#sayIt(CharSequence)}
                         * method passing a word from split arg.
                         */
                        @Override public Object execute() throws GridException {
                            String word = argument(0);

                            // Set session attribute with value of this job's word.
                            ses.setAttribute(jobCtx.getJobId(), word);

                            try {
                                // Wait for all other jobs within this task to set their attributes on
                                // the session.
                                for (GridJobSibling sibling : ses.getJobSiblings()) {
                                    // Waits for attribute with sibling's job ID as a key.
                                    if (ses.waitForAttribute(sibling.getJobId()) == null)
                                        throw new GridException("Failed to get session attribute from job: " +
                                            sibling.getJobId());
                                }
                            }
                            catch (InterruptedException e) {
                                throw new GridException("Got interrupted while waiting for session attributes.", e);
                            }

                            // Create a string containing all attributes set by all jobs
                            // within this task (in this case an argument from every job).
                            StringBuilder msg = new StringBuilder();

                            // Formatting.
                            msg.append("All session attributes [ ");

                            for (Object jobArg : ses.getAttributes().values())
                                msg.append(jobArg).append(' ');

                            // Formatting.
                            msg.append(']');

                            // For the purpose of example, we simply log session attributes.
                            X.println(msg.toString());

                            return sayIt(word);
                        }
                    });
                }

                return jobs;
            }

            /**
             * Sums up all characters from all jobs and returns a
             * total number of characters in the initial phrase.
             *
             * @param results Job results.
             * @return Number of letters for the word passed into
             *      {@link GridSessionExample#sayIt(CharSequence)}} method.
             */
            @Override public Integer reduce(List<GridJobResult> results) {
                return results.size() - 1 + F.sum(F.<Integer>jobResults(results));
            }
        };

        try {
            GridTaskFuture<Integer> f = G.grid().execute(task, "Hello World");

            int phraseLen = f.get();

            X.println(">>>");
            X.println(">>> Finished executing \"Hello World\" example with task session.");
            X.println(">>> Total number of characters in the phrase is '" + phraseLen + "'.");
            X.println(">>> You should see print out of 'Hello' on one node and 'World' on another node.");
            X.println(">>> Afterwards all nodes should print out all attributes added to session.");
            X.println(">>> Check all nodes for output (this node is also part of the grid).");
            X.println(">>>");
        }
        finally {
            G.stop(true);
        }
    }
}
