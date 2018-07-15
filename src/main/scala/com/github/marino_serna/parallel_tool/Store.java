package com.github.marino_serna.parallel_tool;

import java.lang.annotation.*;


@Documented
@Target(ElementType.METHOD)
@Inherited
@Retention(RetentionPolicy.RUNTIME)
/*
 * This annotation will define if the output of the process will be store in an external source (database/file system)
 * or if it is a temporal DataFrame.
 *
 * Not adding the annotation or having temporal as "true" will cache the DataFrame during the execution and delete it after.
 *
 * Having the annotation define with the field temporal as "false" will write de DataFrame in the DataBase/file.
 *
 * The implementation of parallel.Storage provided will handle the writing/reading of the DataFrame, but the implementation suggested is:
 * name => table name or file name
 * schema => dataBase schema or path for the file system.
 */
public @interface Store {

    /**
     * The Output of the Method is temporal, or need to be store in an external source.
     */
    boolean temporal() default false;

    /**
     * Schema of the database, path for the file or equivalent information for storing the DataFrame.
     */
    String schema() default "";

    /**
     * Name of the file, table or equivalent for storing the DataFrame
     */
    String name() default "";

    /**
     * List of fields that will be use for partitioning the table
     */
    String[] partitions() default {};
}
