package com.tools.parallelTool

import java.lang.reflect.Method

import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future, Promise}
import scala.language.{implicitConversions, postfixOps}
import scala.reflect.runtime.universe._
import scala.util.Failure

/**
  * @author Marino Serna Sanmamed
  * @param spark SparkSession
  * @param storage will be use to read and write
  * @param logs print logs for the execution, including duration of the functions
  * @param futuresWithoutDependencies allow to use or not use futures when calling parallelNoDependencies, both solutions are valid
  */
class ParallelTool (spark: SparkSession, storage:Storage, logs:Boolean = false, futuresWithoutDependencies:Boolean=true){
  private val logger: Logger = Logger.getLogger(getClass.getName)
  private var futures:Map[String,Future[DataFrame]] = Map[String, Future[DataFrame]]()
  private var promises:Map[String,Promise[DataFrame]] = Map[String, Promise[DataFrame]]()
  private var futuresFlags:Map[String,Future[Boolean]] = Map[String, Future[Boolean]]()
  private var promisesFlags:Map[String,Promise[Boolean]] = Map[String, Promise[Boolean]]()
  private var functionsToExecute:List[String] = Nil //Used for logs


  /**
    * Inicialize the structures of futures and promises that will be required during the execution.
    * @param functionNames List of names that will be executed
    */
  private def generateFuturesAndPromises(functionNames:List[String]):Unit = {
    def futuresAndPromises(functionNames: List[String]): (Map[String,Future[DataFrame]], Map[String,Promise[DataFrame]],
      Map[String,Future[Boolean]], Map[String,Promise[Boolean]]) = {
      functionNames match {
        case Nil => (Map[String, Future[DataFrame]](), Map[String, Promise[DataFrame]](),
          Map[String, Future[Boolean]](), Map[String, Promise[Boolean]]())
        case _ =>
          val remaining = futuresAndPromises(functionNames.tail)
          val currentFunction = functionNames.head
          val currentPromise:Promise[DataFrame] = Promise[DataFrame]
          val currentFuture = currentPromise.future
          val currentPromiseFlag:Promise[Boolean] = Promise[Boolean]
          val currentFutureFlag = currentPromiseFlag.future
          (remaining._1 + (currentFunction -> currentFuture), remaining._2 + (currentFunction -> currentPromise),
            remaining._3 + (currentFunction -> currentFutureFlag), remaining._4 + (currentFunction -> currentPromiseFlag))
      }
    }
    val mapsReadyToUse = futuresAndPromises(functionNames)
    futures  = mapsReadyToUse._1
    promises = mapsReadyToUse._2
    futuresFlags  = mapsReadyToUse._3
    promisesFlags = mapsReadyToUse._4
  }

  private case class Caller[T>:Null<:AnyRef](klass:T) {

    /**
      * Retrieve the priority from the annotations.
      *
      * Useful to find the critical path of execution and also to optimize the executions by start with the longer tasks
      * @param methodName Name of the Method of which the value of its priority is requested
      * @param args arguments that this method have. Required because function overloading is allowed
      * @return
      */
    def priority(methodName:String,args:AnyRef*):Long={
      def argTypes = args.map(_.getClass)
      def method = klass.getClass.getMethod(methodName, argTypes:_*)
      method.getDeclaredAnnotations.toList.map(annotation => {
        val time:Long = annotation match {
          case priority:PriorityExecution => priority.expectedExecutionTime()
          case _=>0
        }
        time
      }).foldLeft(0L)((processed:Long,current:Long) => processed + current)
    }

    /**
      * Retrieve the dependencies from the annotations.
      *
      * Useful to avoid lockups, also to finding the critical path of execution
      * @param methodName Name of the Method of which the value of its dependencies are requested
      * @param args arguments that this method have. Required because function overloading is allowed
      * @return
      */
    def dependencies(methodName:String,args:AnyRef*):List[String]={
      def argTypes = args.map(_.getClass)
      def method = klass.getClass.getMethod(methodName, argTypes:_*)
      dependencies(method)
    }

    def dependencies(method:Method):List[String]={
      method.getDeclaredAnnotations.toList.map {
        case dependence: DependenceOf => dependence.dependencies().toList
        case _ => Nil
      }.foldLeft(Nil:List[String])((processed,current) => processed ::: current)
    }

    /**
      * Hold on until all the dependencies of the method send by parameter are completed.
      * @param method method that is waiting until all its dependencies are finished
      */
    private def waitFinishAnnotations(method:Method):Unit={
      dependencies(method).map(waitFinish)
    }

    /** call one method of a class using reflexion.
      *
      * This method is designed for Spark and will work only for method that return a DataFrame
      *
      * @param methodName Name of the method that will be executed
      * @param args arguments for the method that will be executed
      * @return The output of the Method that was call. Must be of type DataFrame
      */
    def call[T<:Object, A: TypeTag](methodName:String,args:AnyRef*):DataFrame = {
      try {
        if(logs) logger.info(s"Prepare function: $methodName")
        def argTypes = args.map(_.getClass)
        def method = klass.getClass.getMethod(methodName, argTypes:_*)
        val timeOrig = System.currentTimeMillis()

        waitFinishAnnotations(method)
        if(logs) logger.info(s"In call function: $methodName")

        val dfResult:DataFrame = method.invoke(klass,args: _*).asInstanceOf[DataFrame]//.persist(StorageLevel.OFF_HEAP)
        val timeEnd = System.currentTimeMillis()
        val timeMin = (timeEnd-timeOrig) /1000/60.0
        if(logs) logger.info(f"duration function $methodName: $timeMin%.2f min (${timeEnd-timeOrig}ns)")

        finish(method, methodName, dfResult)

        if(logs) logger.info(s"End call function: $methodName")
        dfResult
      } catch {
        case ex:ClassCastException =>
          val message = s"problem with the result of the function $methodName. The function must return a DataFrame\n ${ex.getCause}"
          logger.error(message)
          ex.printStackTrace()
          System.exit(-1)
          throw new Exception(message)
        case ex: Throwable =>
          logger.error(s"ERROR in method $methodName: ${ex.getMessage}")
          ex.printStackTrace()
          System.exit(-1)
          throw ex
      }
    }

    /**
      * Check if the method exist in the class where is been check
      * @param methodName Name of the method that need to be check
      * @param args arguments for the method that will be executed
      * @return
      */
    def exist(methodName:String,args:AnyRef*):Boolean = {
      klass.getClass.getMethods.exists(x => x.getName.equalsIgnoreCase(methodName))
    }

    /**
      * List with all the functions of this class that have as a parameter the ParallelTool class
      * @return list of names of functions
      */
    def getAllFunctions:List[String]={
      klass.getClass.getMethods.toList.filter(function => {
        function.getParameterTypes.length == 1 && function.getParameterTypes.head.getName == "com.tools.parallelTool.ParallelTool"
      }).map(_.getName)
    }
  }
  private implicit def anyRef2callable[T>:Null<:AnyRef](klass:T):Caller[T] = Caller(klass)

  /**
    * Execute all methods provided as a parameter.
    * @param functions map of classes that contain methods that be executed and all the methods of each class that will be executed
    */

  def startApplication[T<:Object](functions:Map[T, List[String]]):Unit ={
    this.generateFuturesAndPromises(functions.values.toList.flatten)

    /**
      * Update the priority of the list of tasks finding the critical path
      *
      * Every method priority (time that require to be executed) will be increase with the time that takes the predecessors.
      *
      * @param sorted methods that are already updated
      * @param unSorted methods that need to be update
      * @param notPossible number of iterations without changes, to avoid lockups
      * @return List of method with the priority update.
      */
    def updatePriority(sorted:List[(T,String,List[String],Long)],unSorted:List[(T,String,List[String],Long)],
                       notPossible:Int):List[(T,String,List[String],Long)]={
      unSorted match {
        case Nil => sorted
        case head :: tail =>
          val (_,functionName,dependencies,priority):(T,String,List[String],Long) = head
          val irPadre = !unSorted.exists(methodUnSorted => methodUnSorted._3.contains(functionName))
          val currentElement:List[(T,String,List[String],Long)] = head :: Nil
          if(irPadre){
            val updatedPriority = tail.map(x => (x._1,x._2,x._3, if(dependencies.contains(x._2)) x._4 + priority else x._4 ))
            updatePriority(sorted ::: currentElement, updatedPriority,0)
          }else{
            if(notPossible > unSorted.size){
              val error = s"Found a loop with the dependencies: ${unSorted.map(x=>x._2).mkString(" | ")} while updating the priority"
              logger.error(error)
              throw new Exception(error)
            }else{
              updatePriority(sorted, tail ::: currentElement, notPossible + 1)
            }
          }
      }
    }

    /**
      * Sort the functions to avoid lockups and optimice execution time.
      *
      * @param sorted List of methods already sorted
      * @param unSorted List of methods that need to be sorted
      * @param notPossible number of iterations without changes, to avoid lockups
      * @return List of method sorted
      */
    def sortListOfFunctions(sorted:List[(T,String,List[String])],unSorted:List[(T,String,List[String])],notPossible:Int):List[(T,String,List[String])]={
      unSorted match {
        case Nil => sorted
        case remaining :List[(T,String,List[String])] =>
          val (_,_,dependencies):(T,String,List[String]) = remaining.head
          val dependenciesProcessed = dependencies.forall(dependency => sorted.map(x => x._2).contains(dependency))
          val currentElement:List[(T,String,List[String])] = unSorted.head :: Nil
          if(dependenciesProcessed){
            sortListOfFunctions(sorted ::: currentElement, unSorted.tail,0)
          }else{
            val dependenciesPending = dependencies.exists(dependency => unSorted.map(x => x._2).contains(dependency))
            if(dependenciesPending){
              if(notPossible > unSorted.size){
                val error = s"Found a loop with the dependencies: ${unSorted.map(x=>x._2).mkString(" | ")} while sorting functions"
                logger.error(error)
                throw new Exception(error)
              }else{
                sortListOfFunctions(sorted, unSorted.tail ::: currentElement, notPossible + 1)
              }
            }else{
              val error = s"Some of the dependencies: ${dependencies.mkString(" | ")} will not be executed," +
                s"Ensure that the data produced by this methods is available"
              logger.debug(error)
              sortListOfFunctions(sorted ::: currentElement, unSorted.tail,0)
            }
          }
      }
    }

    /**
      * @return List of functions that should be executed
      */
    def getListOfFunctions():List[(T,String,List[String])] = {
      val functionsOutOfOrder = functions.keys.par.map(classToExecute => functions.get(classToExecute) match {
        case Some(listOfFunctions) =>
          functionsToExecute = functionsToExecute ::: listOfFunctions
          listOfFunctions.par.map(functionName => (
            classToExecute,
            functionName,
            classToExecute dependencies (functionName, this),
            classToExecute priority (functionName, this)
          )).toList
        case _ =>
          val error = s"No functions were setup for ${classToExecute.getClass.getName}"
          logger.debug(error)
          Nil
      }).foldLeft(Nil:List[(T,String,List[String],Long)])((ready,current) => ready ::: current)
        .map(x => (x._1,x._2,x._3,x._4))

      updatePriority(Nil,functionsOutOfOrder,0)
        .sortBy(x => -x._4)
        .map(x => (x._1,x._2,x._3))
    }

    val fullListFunctions = getListOfFunctions()

    val listOfFunctionsSorted = sortListOfFunctions(fullListFunctions.filter(x=>x._3.isEmpty),fullListFunctions.filter(x=> x._3.nonEmpty),0)

    if(logs) logger.info(s"${listOfFunctionsSorted.size} execution order: ${listOfFunctionsSorted.map(x=>x._2).mkString(" | ")}")
    val thisParallel = this
    var execution:List[Thread] = listOfFunctionsSorted.map(methodToExecute =>{
      new Thread(new Runnable{def run() {methodToExecute._1 call (methodToExecute._2, thisParallel)}})
    })
    execution.foreach(_.start())

    if(logs) logger.info(s"functions to process: ${execution.size} ")
    while(execution.nonEmpty) {
      execution.head.join()
      execution = execution.filter(fut => fut.isAlive)
      if(logs) logger.info(s"remaining functions to process: ${execution.size} ")
    }

    val functionsWithErrors:List[String] = promisesFlags.keySet.filter(functionName => !isNotFail(functionName)).toList

    if(functionsWithErrors.nonEmpty){
      val error = s"Error during the execution of the functions: ${functionsWithErrors.mkString(" | ")}"
      logger.error(error)
      throw new Exception(error)
    }
  }

  /**
    * Execute all methods that are mark to be executed, that is that have as parameter the class ParallelTool.
    *
    * example of use:
    * val storage:MyStorage = new MyStorage
    * val parallelTool = new ParallelTool(spark, storage)
    * val class1 = new Class1()
    * val class2 = new Class2()
    * val class3 = new Class3()
    * parallelTool.startApplication(class1 :: class2 :: class3 :: Nil)
    *
    * @param executionClasses Classes where to look for methods
    */
  def startApplication[T<:Object](executionClasses:List[Object]):Unit = {
    startApplication(executionClasses.map(currentClass => currentClass -> currentClass.getAllFunctions).toMap)
  }

  /**
    * Wait until the method is compleated and check if the execution fail.
    * @param functionName Name of the method to check
    * @return true if the execution is correct, false if fail.
    */
  private def isNotFail(functionName:String):Boolean={
    futuresFlags.get(functionName) match {
      case Some(promise) =>
        if (promise.isCompleted){
          promise.onFailure{
            case exception =>
              val error = s"Error in the function: $functionName fail during the execution. ${exception.getMessage}}"
              exception.printStackTrace()
              logger.error(error)
              throw new Exception(error)
          }
          Await.result(promise, Duration.Inf)
        }else{
          true
        }
      case _ =>
        false
    }
  }

  /**
    * Check if the function has been completed and the result can be use by other functions.
    * @param functionName Name of the function that need to be check
    * @return true when the function is ready
    */
  private def waitFinish(functionName:String):Boolean={
    futuresFlags.get(functionName) match {
      case Some(promise) =>
        if(logs) logger.info(s"in waiting($functionName)")
        val res:Boolean = Await.result(promise, Duration.Inf)
        if(logs) logger.info(s"out waiting($functionName)")
        res
      case _ =>
        if(functionsToExecute.contains(functionName)){
          val error = s"Error with the execution of the function: $functionName, flag is not available"
          logger.error(error)
          throw new Exception(error)
        }else{
          val error = s"The function: $functionName is not scheduled to be executed, " +
            s"Ensure that the data produced by this methods is available." +
            s"The full list of functions to be executed is: ${functionsToExecute.mkString(" | ")}}"
          logger.debug(error)
          true
        }
    }
  }

  /**
    * Check the annotations and return the schema/folder, the table name/file name and the partitions (if required)
    * if a empty list is return means that the result will be store in a temporal location.
    *
    * This values will be interpreted be the implementation of Store, the uses mention above are suggestions.
    * @param method Name of the method to check
    * @return (Schema, name, partitions) :: Nil
    */
  def getSource(method:Method):List[(String,String, List[String])]={
      method.getDeclaredAnnotations.toList.map {
        case schema: Store =>
          if (schema.temporal()) {
            Nil
          } else if (schema.schema().isEmpty || schema.name().isEmpty) {
            Nil
          } else {
            (schema.schema(), schema.name(), schema.partitions().toList) :: Nil
          }
        case _ => Nil
      }.foldLeft(Nil:List[(String,String,List[String])])((accumulated, current) => accumulated ::: current)
  }

  /**
    * Notify that the method has been executed, and store the result.
    *
    * If the difference of time required to read from the disk of the workers and the time required to read from the persistent storage is big
    * you can force to write all the time in the temporal storage and after updating the flags write in the persistent storage.
    *
    * This application has been tested in a environment with regular/slow hard drives in the cluster workers/master,
    * but a pretty fast persistence environment (google cloud storage), if you have fast SSD hard drives in your workers,
    * or your persistent storage is slower than the hard drives used in your cluster, you should test that optimization.
    *
    * Please do not be tempted to use cache to store the DataFrames, instead of StorageLevel.OFF_HEAP,
    * because spark will consider that you are not using that DataFrames and sometimes you will lost it.
    *
    * @param method method that has been executed
    * @param functionName name of the method that has been executed
    * @param value
    */
  private def finish(method: Method, functionName:String, value:DataFrame):Unit={
    getSource(method) match{
      case Nil =>
        value.persist(StorageLevel.OFF_HEAP)
      case (schemaName,tableName, partitions) :: _ =>
        storage.write(schemaName, tableName, value, partitions)
    }
    def error():Unit={
      // this could be and error, but also happens in a correct execution, like when using parallelNoDependencies
      val error = s"One function is trying write a value in the name of a function ($functionName) that is not plan to be executed, " +
        s"please review the list of functions to execute and the functions that addData to ($functionName)"
      logger.debug(error)
    }

    promises.get(functionName) match {
      case Some(promise) =>
        promise success value
      case _ =>
        error()
    }
    promisesFlags.get(functionName) match {
      case Some(promise) =>
        promise success true
      case _ =>
        error()
    }
  }

  /**
    * Once one method is completed, the future is completed. using this method is possible to retrieve the result of the completed method.
    * @param methodName Method name of which the output is requested
    * @param klass class where the method can be found
    * @return DataFrame with the result of the method
    */
  def get(methodName:String, klass:AnyRef):DataFrame = {

    def getParallelMethod():Method={
      def getParallelMethod(args:AnyRef*):Method={
        def argTypes = args.map(_.getClass)
        klass.getClass.getMethod(methodName, argTypes:_*)
      }
      val thisParallel = this
      getParallelMethod(thisParallel)
    }

    if(logs) logger.info(s"get($methodName, ${klass.getClass.getName})")

    def method = getParallelMethod()
    waitFinish(methodName)
    val timeOrig = System.currentTimeMillis()
    val finalRes = try {
      getSource(method) match{
        case Nil =>
          futures.get(methodName) match {
            case Some(promise) =>
              Await.result(promise, Duration.Inf)
            case _ =>
              val error = s"The function: $methodName need to be executed, please review the list of functions" +
                s" to execute: ${functionsToExecute.mkString(" | ")}}"
              logger.error(error)
              throw new Exception(error)
          }
        case (schemaName,tableName, _) :: _ => storage.read(schemaName,tableName)
      }
    }catch{
      case ex:Throwable =>
        if(logs) logger.error(f"catch from disk get($methodName)")
        ex.printStackTrace()
        throw ex
    }
    val timeEnd = System.currentTimeMillis()
    val timeMin = (timeEnd-timeOrig) /1000/60.0
    if(logs) logger.info(f"duration get($methodName): $timeMin%.2f min (${timeEnd-timeOrig}ns)")
    finalRes
  }

  /**
    * This Method allow to execute in parallel a list of methods without dependencies.
    *
    * This logic is far simpler and works great when inside one method is required to process a list of methods and use the results only in that method.
    * The output of this methods has been tested in conditions where is possible to fit the DataFrames in memory,
    * the use of masibe DataFrames in relation with the cluster size, could requires more testing.
    *
    * Is possible to use bot parallel systems at the same time, in fact is recommended.
    *
    * I recommend to use the default configuration, without futures, in my experience works faster, but if in some scenarios happen to be different
    * the solution with futures is also implemented.
    *
    * Example of use:
    *
    * val functionsToExecute =
    * ("function1", parameter1a :: Nil) ::
    * ("function2", parameter2a :: parameter2b :: parameter2c :: Nil) ::
    * ("function3", parameter3a :: parameter3b :: Nil) ::
    * Nil
    * val (resultDF1 ::
    * resultDF2 ::
    * resultDF3 ::
    * _ ) = parallelTool.parallelNoDependencies(this, functionsToExecute)
    *
    * @param classToExecute class that contains the methods that need to be executed.
    * @param functions list of functions that will be executed
    * @return list of DataFrames containing all the results
    */
  def parallelNoDependencies[T<:Object, A: TypeTag](classToExecute:T, functions:List[(String,List[AnyRef])]):List[DataFrame] ={
    val dfResults = if(futuresWithoutDependencies){
      if(logs) logger.info(s"parallelNoDependenciesWithoutFutures: ${functions.mkString(" | ")}")
      parallelNoDependenciesWithoutFutures(classToExecute, functions)
    }else{
      if(logs) logger.info(s"parallelNoDependenciesWithFutures: ${functions.mkString(" | ")}")
      parallelNoDependenciesWithFutures(classToExecute, functions)
    }
    if(logs) logger.info(s"End parallelNoDependencies : ${functions.mkString(" | ")}")
    dfResults
  }

  private def parallelNoDependenciesWithFutures[T<:Object, A: TypeTag](classToExecute:T, functions:List[(String,List[AnyRef])]):List[DataFrame] = {
    val futureList:List[(String,Future[DataFrame])] = functions.par.map( toExecute =>{
      val futureOfFunction:(String,Future[DataFrame]) = toExecute match{
        case (functionName,parameters) =>
          val f: Future[DataFrame] = Future {
            classToExecute call (functionName, parameters:_*)
          }
          (functionName,f)
        case _ =>
          logger.error("problem with functionName and parameters")
          null
      }
      futureOfFunction }

    ).filter(_ != null).toList

    val resultDF = futureList.par.map {
      case (executedFunction, futureDF: Future[DataFrame]) =>
        val resultFunction: DataFrame = Await.result(futureDF, Duration.Inf)
        futureDF.onComplete {
          case Failure(error) => logger.error(s"An error has occurred with the function $executedFunction: ${error.getMessage}")
          case _ =>
        }
        resultFunction
      case _ => null
    }.filter(_ != null).toList

    if(resultDF.length != functions.length){
      val error = s"${resultDF.length} != ${functions.length} One of the method is not returning a DataFrame  ${functions.map(x => x._1).mkString(" | ")}"
      logger.error(error)
      throw new Exception(error)
    }
    resultDF
  }

  private def parallelNoDependenciesWithoutFutures[T<:Object, A: TypeTag](classToExecute:T, functions:List[(String,List[AnyRef])]):List[DataFrame] ={
    val resultDF:List[DataFrame] = functions.par.map {
      case (functionName, parameters) => classToExecute call(functionName, parameters: _*)
      case _ =>
        logger.error("problem with functionName and parameters")
        null
    }.filter(_ != null).toList

    if(resultDF.length != functions.length){
      val error = s"${resultDF.length} != ${functions.length} One of the method is not returning a DataFrame  ${functions.map(x => x._1).mkString(" | ")}"
      logger.error(error)
      throw new Exception(error)
    }
    resultDF
  }
}
