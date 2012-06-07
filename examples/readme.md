![GridGain Logo](http://www.gridgain.com/images/logo/logo_mid.png "GridGain Logo")

GridGain is JVM-based open source middleware for **in-memory big data processing** that scales to terabytes of data and thousands of machines. It uniquely integrates world's fastest MapReduce implementation with fully transactional In-Memory Data Grid.

## GridGain Examples
This folder contains all examples shipped with GridGain including examples for Java, Scala, Groovy, PHP with REST access as well as configuration and other miscellaneous files.

> If you are using IDEA or Eclipse Java IDEs you can open pre-configured projects by either clicking on `idea_users_open_this_file.ipr` file or following instructions in `eclipse_users_read_here.txt`. 
> 
>
> Both files are located in the root of GridGain installation.

### GRIDGAIN_HOME variable
Don't forget to setup `GRIDGAIN_HOME` environment variable pointing to GridGain installation root. 

### 'config' folder
This folder contains GridGain XML configuration files. You'll need various configuration file (i.e. settings) to run various example. Each example always has documentation on what specific configuration (i.e. configuration file) it requires.

### 'libs' folder
This folder contains necessary libraries for example. For example, it contains Hadoop HDFS library for Hadoop examples.

### 'keystore' folder
This folders contains files required by SSL for Java Client example.

### 'dotnet' folder
This folders contains examples for .NET/C# Client APIs.

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


### 'grover' folder
This folder contains `Grover` examples. Grover is Groovy++ based DSL for GridGain.

This is the simple example of full featured MapReduce app in Grover (equivalent to Java example above):

    package org.gridgain.grover.examples

    import static org.gridgain.grid.GridClosureCallMode.*
    import static org.gridgain.grover.Grover.*
    import org.gridgain.grover.categories.*

    @Typed
    @Use(GroverProjectionCategory)
    class GroverWorldShortestMapReduce {
        static void main(String[] args) {
            grover { ->
                def input = "World shortest mapreduce application"

                println("Non-space characters count: " +
                    grid$.reduce$(SPREAD, input.split(" ").
                    	collect { { -> it.length() } }, { it.sum() })
                )
            }
        }
    }


### 'scalar' folder
This folder contains `Scalar` examples. Scalar is an advanced Scala-based DSL for GridGain.

This is the simple example of full featured MapReduce app in Scalar (equivilant to Java example above):
 
    package org.gridgain.scalar.examples
	
    import org.gridgain.scalar.scalar
    import scalar._
    import org.gridgain.grid.GridClosureCallMode._
    
    object ScalarWorldShortestMapReduce {
        def main(args: Array[String]) {
            scalar {
                val input = "World shortest mapreduce application"
    
                println("Non-space characters count: " +
                    grid$ @<(SPREAD, for (w <- input.split(" ")) 
                        yield (() => w.length), (s: Seq[Int]) => s.sum)
                )
            }
        }    
    }
	

### 'gar' folder
This folder contains example of deploying GridGain compute task using GAR file protocol.

### 'php' folder
This folder contains PHP example of accessing GridGain via REST APIs as well as using `memcached` protocol for accessing GridGain's In-Memory Data Grid.



