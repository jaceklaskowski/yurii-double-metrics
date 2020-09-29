import io.delta.tables.DeltaTable
import org.apache.spark.sql._
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode, Trigger}

object DoubleMetricsApp extends App {

  val spark = SparkSession.builder().master("local[*]").getOrCreate()
  import spark.implicits._

  println(s">>> Using Spark ${spark.version}")

  case class Input(key: Long, value: Long)

  import org.apache.spark.sql.functions._
  val inputStream = spark
    .readStream
    .format("socket")
    .options(Map("host" -> "localhost", "port" -> "9999"))
    .load
    .withColumn("key", split($"value", ",")(0) cast "long")
    .withColumn("value", split($"value", ",")(1) cast "long")
    .as[Input]

  val inputBatch = Seq((1,1), (1,2)).toDF("key", "value").as[Input]

  val input = inputBatch

  case class Session(values: Seq[Long])
  def stateUpdateFunc(key: Long, inputs: Iterator[Input], state: GroupState[Session]): Iterator[(Long, Seq[Long])] = {
    val values = inputs.map(_.value).toArray.toSeq
    var result: Iterator[(Long, Seq[Long])] = Iterator((key, values))
    println(s">>> >>> stateUpdateFunc executed -> key=$key, values: $values")
    if (state.hasTimedOut) {
      println(s">>> >>> >>> 1. state has timed out => removing it")
      println(s">>> >>> >>> >>> valuesByKey = ${state.get.values}")
      state.remove()
      result = Iterator.empty
    } else if (state.exists) {
      println(s">>> >>> >>> 2. state exists")
      println(s">>> >>> >>> >>> (before) valuesByKey = ${state.get.values}")
      val existingState = state.get
      val newState = existingState.copy(values = existingState.values ++ values)
      state.update(newState)
      state.setTimeoutDuration("15 seconds")
      println(s">>> >>> >>> >>> (after)  valuesByKey = ${state.get.values}")
      result = Iterator((key, state.get.values))
    } else {
      println(s">>> >>> >>> 3. no earlier state")
      val initialState = Session(values = values)
      state.update(initialState)
      state.setTimeoutDuration("15 seconds")
      result = Iterator((key, state.get.values))
    }
    result
  }
  val outputMode = OutputMode.Update
  val transformed = input.groupByKey(_.key)
    .flatMapGroupsWithState(
      outputMode,
      GroupStateTimeout.ProcessingTimeTimeout)(stateUpdateFunc)
    .toDF("key", "values")

  val deltaPath = "/tmp/yurii-delta-double-metrics"
  println(s"Create an empty Delta table at $deltaPath")
  spark.emptyDataset[(Long, Seq[Long])]
    .toDF("key", "values")
    .write
    .mode(SaveMode.Overwrite)
    .option("mergeSchema", true)
    .format("delta")
    .save(deltaPath)

  DeltaTable.forPath(deltaPath).as("existing")
    .merge(transformed.as("updates"), $"existing.key" === $"updates.key")
    .whenNotMatched().insertAll()
    .whenMatched().updateAll()
    .execute()

  val duration = 10
  println(s"Sleeping for $duration")
  java.util.concurrent.TimeUnit.MINUTES.sleep(duration)

  val batch = true
  val streaming = !batch
  if (streaming) {
    import concurrent.duration._
    transformed
      .writeStream
      .outputMode(outputMode)
      .trigger(Trigger.ProcessingTime(5.seconds))
      .foreachBatch { (df: DataFrame, batchId: Long) =>
        println(s">>> foreachBatch (batchId: $batchId)")
        df.show(truncate = false)

        println(s">>> >>> MERGE INTO Delta table: $deltaPath")
        DeltaTable.forPath(deltaPath).as("existing")
          .merge(df.as("updates"), $"existing.key" === $"updates.key")
          .whenNotMatched().insertAll()
          .whenMatched().updateAll()
          .execute()
        println(s">>> foreachBatch (batchId: $batchId)...DONE")
        println("")
      }
      .start
      .awaitTermination
  }
}

