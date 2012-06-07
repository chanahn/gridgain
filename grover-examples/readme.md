![GridGain Logo](http://www.gridgain.com/images/logo/logo_mid.png "GridGain Logo")

GridGain is JVM-based open source middleware for **in-memory big data processing** that scales to terabytes of data and thousands of machines. It uniquely integrates world's fastest MapReduce implementation with fully transactional In-Memory Data Grid.

## GridGain Examples
This folder contains all examples shipped with GridGain including examples for Java, Scala, Groovy, PHP with REST access as well as configuration and other miscellaneous files.

> If you are using IDEA or Eclipse Java IDEs you can open pre-configured projects by either clicking on `idea_users_open_this_file.ipr` file or following instructions in `eclipse_users_read_here.txt`. 
> 
>
> Both files are located in the root of GridGain installation.

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
