![GridGain Logo](http://www.gridgain.com/images/logo/logo_mid.png "GridGain Logo")

GridGain is JVM-based open source middleware for **in-memory big data processing** that scales to terabytes of data and thousands of machines. It uniquely integrates world's fastest MapReduce implementation with fully transactional In-Memory Data Grid.


## GridGain Examples
This folder contains all examples shipped with GridGain including examples for Java, Scala, Groovy, PHP with REST access as well as configuration and other miscellaneous files.

> If you are using IDEA or Eclipse Java IDEs you can open pre-configured projects by either clicking on `idea_users_open_this_file.ipr` file or following instructions in `eclipse_users_read_here.txt`. 
> 
>
> Both files are located in the root of GridGain installation.


### 'java' folder
This folder contains Java examples for all major GridGain APIs. There are over 50 different examples for different GRidGain subsystems.

This is the simple example of full featured MapReduce app in Java using GridGain's FP APIs:

    package org.gridgain.examples.helloworld.api30;

    import org.gridgain.grid.*;
    import org.gridgain.grid.lang.*;
    import org.gridgain.grid.typedef.*;
    import java.util.*;
    import static org.gridgain.grid.GridClosureCallMode.*;

    public class GridFunctionalMapReduceExample {
        public static void main(final String[] args) throws GridException {
            if (args.length == 1 && !args[0].isEmpty())
                G.in(new GridInClosureX<Grid>() {
                    @Override public void applyx(Grid g) throws GridException {
                        X.println("Length of input argument is " + g.reduce(
                            SPREAD,
                            new GridClosure<String, Integer>() {
                                @Override public Integer apply(String s) {
                                    System.out.println("Calculating for: " + s);

                                    return s.length();
                                }
                            },
                            //F.<String, Integer>cInvoke("length"),
                            Arrays.asList(args[0].split(" ")),
                            F.sumIntReducer()
                        ));
                    }
                });
        }
    }

