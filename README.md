
# ParallelTool

## Introduction
Tool design to speed up applications and increase the intensity of use the cluster.

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


## How to use this tool?

How to use this tool?

1.	Create your personal storage class, by extending the class: “com.tools.parallelTool.Storage”
In this class will implement how the application persist data, and how the application read data already persisted.

2.	Locate and adapt all the methods that will be queued to be executed:

  a.	That is all main methods of the application. Ideally every one of these methods will produce one DataFrame that is use by other methods or will be persisted.

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

    v.	Optional but recommended, adding an annotation with the time that this method takes to be executed,
        if the application is executed with the logs activated in parallelTool, the real value will be printed.
        This is use to optimize the sorting of methods and finding critical paths. Example:
        @PriorityExecution(expectedExecutionTime = 66036)

3.  The application will be started like:

        val storage:MyStorage = new MyStorage
        val parallelTool = new ParallelTool(sparkSession, storage)
        val classToExecute1 = new classToExecute1()
        val classToExecute2 = new classToExecute2()
        val classToExecute3 = new classToExecute3()
        parallelTool.startApplication(classToExecute1 :: classToExecute2 :: classToExecute3 :: Nil)



