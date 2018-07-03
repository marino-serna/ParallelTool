# ParallelTool
Tool design to speed up applications and increase the intensity of use the cluster.

The target of this tool is optimizing the use of the cluster, increasing the parallelization that spark does.

This tool tries to increase the number of task that an application runs at the same time in a cluster, starting with the tasks that will delay the execution (critical path), and avoiding blocked task waiting for resources.
when an application is executed alone in a cluster, is common to detect a very irregular use of the processor, sometimes even with areas of very low activity.
The idea behind this tool is to fill some of this area with small task that will be need in the future.

You may think that spark is already parallelizing, and that is true, spark does a very good job but in a lot of situations is possible to get it better.
The reduction of time that you will enjoy depends in what are you processing, if you have lots of inputs and different logics that are independent up to one point, like a tree, the improvement will be big. If what you are processing is purely linear you will get a poor improvement.
Also depending on the cluster that is available, the application will have more margin of improvement:
-	Number of cores: will be decisive defining the number of task that will be executed in parallel.
-	Current average use of the cluster: if the cluster is already intensely use there is less opportunities to improve.

