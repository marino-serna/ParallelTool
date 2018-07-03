package com.tools.parallelTool;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;


@Documented
@Target(ElementType.METHOD)
@Inherited
@Retention(RetentionPolicy.RUNTIME)
/*
 * If provided, allow ParallelToll to sort the execution of methods base in the dependencies.
 * This annotation will speed up the execution and avoid blockades
 */
public @interface DependenceOf {
    String[] dependencies();
}
