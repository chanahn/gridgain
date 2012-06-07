![GridGain Logo](http://www.gridgain.com/images/logo/logo_mid.png "GridGain Logo")

**GridGain is Java based open source middleware for real time big data processing and analytics that scales up from one server to thousands of machines.**

## GridGain Examples
This folder contains all examples shipped with GridGain including examples for Java, Scala, Groovy, PHP with REST access as well as configuration and other miscellaneous files.

> If you are using IDEA or Eclipse Java IDEs you can open pre-configured projects by either clicking on `idea_users_open_this_file.ipr` file or following instructions in `eclipse_users_read_here.txt`. 
> 
>
> Both files are located in the root of GridGain installation.

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
	
