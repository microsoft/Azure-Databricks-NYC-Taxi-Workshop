// Databricks notebook source
//1.  Dataset to be scored
val scorableDF = spark.sql("select * from taxi_db.model_raw_trips")

// COMMAND ----------

//2.  Transform for scoring
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.feature.OneHotEncoderEstimator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.feature.Normalizer
import org.apache.spark.ml.feature.StringIndexer

val indexerPickup = new StringIndexer()
                   .setInputCol("pickup_locn_id")
                   .setOutputCol("pickup_locn_id_indxd")
                   .fit(scorableDF)
val indexerDropoff= new StringIndexer()
                    .setInputCol("dropoff_locn_id")
                    .setOutputCol("dropoff_locn_id_indxd")
                    .fit(scorableDF)
val indexerRatecode= new StringIndexer()
                    .setInputCol("rate_code_id")
                    .setOutputCol("rate_code_id_indxd")
                    .fit(scorableDF)
val indexerStoreFlag=new StringIndexer()
                    .setInputCol("store_and_fwd_flag")
                    .setOutputCol("store_and_fwd_flag_indxd")
                    .fit(scorableDF)

val indexedDF=indexerPickup.transform(
                   indexerDropoff.transform(
                     indexerStoreFlag.transform(
                       indexerRatecode.transform(scorableDF))))

val encoder = new OneHotEncoderEstimator()
  .setInputCols(Array("pickup_locn_id_indxd", "dropoff_locn_id_indxd","rate_code_id_indxd","store_and_fwd_flag_indxd"))
  .setOutputCols(Array("pickup_locn_id_encd", "dropoff_locn_id_encd","rate_code_id_encd","store_and_fwd_flag_encd"))
  .fit(indexedDF)

val encodedDF = encoder.transform(indexedDF)
val featureCols=Array("pickup_locn_id_encd","trip_distance","pickup_hour","pickup_day","pickup_day_month","pickup_minute","pickup_weekday","pickup_month","rate_code_id_encd")
val assembler = new VectorAssembler()
                .setInputCols(featureCols)
                .setOutputCol("features")

val assembledDF = assembler.transform(encodedDF)
val assembledFinalDF = assembledDF.select("duration","features")

val normalizedDF = new Normalizer()
  .setInputCol("features")
  .setOutputCol("normFeatures")
  .transform(assembledFinalDF)

val labelIndexer = new StringIndexer().setInputCol("duration").setOutputCol("duration_indxd_label")

//Final dataframe - ready for scoring
val scoringReadyDF = labelIndexer.fit(normalizedDF).transform(normalizedDF)

//Persist to storage
val scoreReadyDestinationDir = "/mnt/workshop/curated/nyctaxi/model-score-ready/trips/""
scoringReadyDF.write.mode(SaveMode.Overwrite).save(destDataDirRootTrips)

// COMMAND ----------

//  Load model from DBFS
import org.apache.spark.ml.regression.RandomForestRegressionModel
val rfModel = RandomForestRegressionModel.load("/mnt/workshop/consumption/nyctaxi/model/duration-pred/")

// COMMAND ----------

// Read stream the scoring ready dataset
import org.apache.spark.ml.linalg.{VectorUDT,Vectors}
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType 

val schema = new StructType()
  .add(StructField("duration", LongType))
  .add(StructField("features",VectorType))
  .add(StructField("normFeatures",VectorType))
  .add(StructField("actuals", DoubleType))

val streamingData = spark.readStream
  .schema(schema)
  .option("maxFilesPerTrigger", 1)
  .parquet(scoreReadyDestinationDir)

display(streamingData)

// COMMAND ----------

//  Predict on events in the strea,
val rfPredictions = rfModel.transform(streamingData)
                           .select($"actuals".cast(IntegerType),$"prediction".cast(IntegerType))
 .groupBy($"prediction",$"actuals").agg(abs($"prediction"-$"actuals").as("diff")).orderBy($"diff".asc).drop($"diff")

// Display stream
display(rfPredictions)