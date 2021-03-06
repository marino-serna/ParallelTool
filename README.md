
# ParallelTool

## Introduction
Tool design to speed up Spark applications and increase the intensity of use the cluster.

The target of this tool is optimizing the use of the cluster, increasing the parallelization that spark does.

## What this tool does?

This tool tries to increase the number of task that an application runs at the same time in a cluster, starting with the tasks that will delay the execution (critical path), and avoiding blocked task waiting for resources.
when an application is executed alone in a cluster, is common to detect a very irregular use of the resources, sometimes even with areas of very low activity.
The idea behind this tool is to fill some of this area with small task that will be need in the future.

You may think that spark is already parallelizing, and that is true, spark does a very good job but in a lot of situations is possible to do it better.
Each application will get different improvement by using this took:
-	Applications with task that are not linear dependent works better. If the dependencies of the task look like a tree or a set of interconnected trees, the application will be a good candidate for the tool.
This is a common situation in ETLs where there are plenty of inputs and outputs.
-	Applications with only one thread of logic are harder to adapt to this tool, and the improvement will be smaller.
For example, if over one data set the application performs very complex mathematical functions, that all the time require the output of the previous step.

Also depending on the cluster that is available, the application will have more margin of improvement:
-	Number of cores: will be decisive defining the number of task that will be executed in parallel.
-   Current average use of the cluster: if the cluster is already under intense use, there are less opportunities to improve by using unused resources.

## Example of a project using ParallelTool

https://github.com/marino-serna/ParallelToolExample.git

## How to use this tool?

How to use this tool?

1. Add the dependencies in to your project:
  - https://mvnrepository.com/artifact/com.github.marino-serna/parallel-tool/1.0.1-00

2. Import the required classes:
  - import com.github.marino_serna.parallel_tool._

3.	Create your own storage class, by extending the class: “com.github.marino_serna.parallel_tool.Storage”
This class will implement how the application persist data, and how the application read data already persisted.
A useful example of implementation can be found in:
  - com.github.marino_serna.parallel_tool.DataBaseStorage

4.	Locate and adapt all the methods that will be queued to be executed:

  a.	That is all _main_ methods of the application. Ideally every one of these methods will produce one DataFrame that is use by other methods or will be persisted.

  b.	These methods:

    i.	Must receive as parameter “ParallelTool”, and nothing else

    ii.	Every previous parameter will be adapted as:

    val parameterName = parallelTool.get("nameOfTheMethodThatProduceThisParameter”, instanceOfTheClassWhereThisMethodIsLocated)

    Is important to mention that the Method that produce this parameter must be one of the main methods.

    iii.	For every entry add to one method in the previous step, step “ii”, a dependence will be added as a comment
        @DependenceOf(dependencies = Array("nameOfMethodThatGenerateTheDependence"))

    iv.	The Method must return a DataFrame, if the original method was persisting that DataFrame will use an annotation like:
        @Store(schema = “schemaOrFolder”, name = “tableNameOrFileName”, partitions = Array("ifUseAllTheColumnsThatWillBeUseToStorePartitions"))
        Otherwise  will use an annotation like:
        @Store(temporal = true)

    v.	Since the tag 1.0.1-02 the system will update the priority of execution base of information of previous executions
        if a schema/folder to store this information is provide: “storePrioritySchema”,
        the table/filename “parallelToolPriority” will be unless a different name is provided.
        Is also possible to force the priorities using annotations and not defining a value for “storePrioritySchema”, in that scenario the annotation should contain the time that this method takes to be executed, except for the methods that want to be prioritized or delayed.
        The time that each method takes to be executed can be found in the logs of the application if the application is executed with the logs activated.
        This annotation is use by the application to optimize the sorting of methods and finding critical paths. Example:
        @PriorityExecution(expectedExecutionTime = 66036)

5.  The application will be started as follow:

        val storage:MyStorage = new MyStorage
        val parallelTool = new ParallelTool(sparkSession, storage)
        val classToExecute1 = new classToExecute1()
        val classToExecute2 = new classToExecute2()
        val classToExecute3 = new classToExecute3()
        parallelTool.startApplication(classToExecute1 :: classToExecute2 :: classToExecute3 :: Nil)


## Extra functionality

Some methods require the execution of a set of methods that produce temporal DataFrames that no one else will use.
In this situation is possible to use functionality “parallelNoDependencies”.
This functionality is useful only in a very limited scenario, assuming that the target is produce the most optimal application that is possible, but that times is better to use it

This functionality has some advantages:
-	Faster: is using only memory, and the implementation allow faster execution of the code.
-	Clarity: make easy to everyone that read the method that these DataFrames won’t be use outside of this Method.

On the other hand, have some disadvantages:
-	Memory: all the DataFrames will be stored in memory during the execution, that means that if can’t fit on memory you could have risk of reprocessing the DataFrame.
This issue is theatrical, during my tests that never happen but be aware of the risk.
-	If one of the DataFrames that is processed required far more time that the rest of the process this could delay the execution. Using this functionality this set of methods will start at the same time, and the application will continue after all are completed.

* The functionality will be use as follow:

        val functionsToExecute =
        ("function1", parameter1a :: Nil) ::
        ("function2", parameter2a :: parameter2b :: parameter2c :: Nil) ::
        ("function3", parameter3a :: parameter3b :: Nil) ::
        Nil

        val (resultDF1 ::
        resultDF2 ::
        resultDF3 ::
        _ ) = parallelTool.parallelNoDependencies(this, functionsToExecute)
