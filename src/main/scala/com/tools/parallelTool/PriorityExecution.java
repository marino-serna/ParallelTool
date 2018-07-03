package com.tools.parallelTool;

import java.lang.annotation.*;


@Documented
@Target(ElementType.METHOD)
@Inherited
@Retention(RetentionPolicy.RUNTIME)
/*
 * If provided, allow ParallelToll to sort the execution of methods base in the time that is required to be executed.
 * This annotation will by apply as a secondary sort after "DependenceOf"
 * The logic applied will start with the longer process, to maximize the number of process running
 * The value of this annotation should be a estimation of the time that the process take to be perform.
 * introduce a bigger value in this annotation is the recommended way of prioritize one method over the remaining methods
 */
public @interface PriorityExecution {
    long expectedExecutionTime() default 0;
}
