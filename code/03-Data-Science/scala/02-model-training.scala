// Databricks notebook source
// MAGIC %md 
// MAGIC # Supervised learning | Use case: Trip duration prediction | Model training with Spark MLlib
// MAGIC The goal of this tutorial is build on the data engineering lab and learn how to -
// MAGIC 1.  Pre-process the NYC taxi dataset
// MAGIC 2.  Determine feature importance
// MAGIC 3.  Transform features for model training using Spark MLlib built-in functions
// MAGIC 4.  Train model in two algorithms - Logistic Regression and Random Forest Regression
// MAGIC 5.  Evaluate model across the two algorithms
// MAGIC 6.  Persist model to storage for batch scoring at a later time

// COMMAND ----------

// MAGIC %md ### 1. Base dataset review

// COMMAND ----------

//1.  Base dataset
val baseDF = spark.sql("select * from taxi_db.model_raw_trips")

// COMMAND ----------

baseDF.printSchema

// COMMAND ----------

display(baseDF)

// COMMAND ----------

baseDF.describe().show()

// COMMAND ----------

// MAGIC %md ### 2. Determining features of importance 
// MAGIC Correlation is used to test relationships between quantitative variables or categorical variables. In other words, it’s a measure of how things are related. Correlations are useful because if you can find out what relationship variables have, you can make predictions about future behavior.
// MAGIC 
// MAGIC Spark ML provides us with library functions to perform correlation and other basic statistic operations.

// COMMAND ----------

// MAGIC %md Correlation coefficients range between -1 and +1. The closer the value is to +1, the higher the positive correlation is, i.e the values are more related to each other. The closer the correlation coefficient is to -1, the more the negative correlation is. It can be mentioned here that having a negative correlation does not mean the values are less related, just that one variable increases as the other decreases.
// MAGIC 
// MAGIC For example, a correlation of -0.8 is better than a correlation of 0.4 and the values are more related in the first case than in the second.

// COMMAND ----------

import org.apache.spark.ml.linalg.{Matrix, Vectors}
import org.apache.spark.ml.stat.Correlation
import scala.collection.mutable.ListBuffer
import scala.collection.immutable.ListMap
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

val ls= new ListBuffer[(Double, String)]()
println("\nCorrelation Analysis :")
   	for ( field <-  baseDF.schema) {
		if ( ! field.dataType.equals(StringType)) {
          var x= baseDF.stat.corr("duration", field.name)
          var tuple : (Double, String) = (x,field.name)
          ls+=tuple
		}
	}
//Let us sort the correlation values in descending order to see the fields in the order of their relation with the duration field
// Sorting our correlation values in descending order to see the most related fields first
val lsMap= ls.toMap
val sortedMap= ListMap(lsMap.toSeq.sortWith(_._1 > _._1):_*)
sortedMap.collect{
  case (value, field_name) => println("Correlation between duration_minutes and " + field_name + " = " + value)
}

// COMMAND ----------

// MAGIC %md 
// MAGIC #### Conclusion
// MAGIC From our analysis, understand the features of importance relative to `duration` column 

// COMMAND ----------

// MAGIC %md ### 3. Feature Transformation

// COMMAND ----------

// MAGIC %md 
// MAGIC Since we are going to try algorithms like Linear Regression, *we need all the categorical variables in the dataset as numeric variables*. <br>
// MAGIC There are 2 options:
// MAGIC 
// MAGIC **1.  Category Indexing**
// MAGIC 
// MAGIC This is basically assigning a numeric value to each category from {0, 1, 2, ...numCategories-1}. This introduces an implicit ordering among your categories, and is more suitable for ordinal variables (eg: Poor: 0, Average: 1, Good: 2)
// MAGIC 
// MAGIC **2.  One-Hot Encoding**
// MAGIC 
// MAGIC This converts categories into binary vectors with at most one nonzero value (eg: (Blue: [1, 0]), (Green: [0, 1]), (Red: [0, 0]))

// COMMAND ----------

// MAGIC %md #### 3.1. Indexing
// MAGIC 
// MAGIC StringIndexer encodes a string column of labels to a column of label indices. The indices are in [0, numLabels), ordered by label frequencies, so the most frequent label gets index 0. The unseen labels will be put at index numLabels if user chooses to keep them. If the input column is numeric, we cast it to string and index the string values. When downstream pipeline components such as Estimator or Transformer make use of this string-indexed label, you must set the input column of the component to this string-indexed column name. In many cases, you can set the input column with setInputCol.
// MAGIC 
// MAGIC The dataset has categorical data in the pickup_location_id, dropoff_location_id, store_and_fwd_flag and rate_code_id columns. We will use a string indexer to convert these to numeric labels

// COMMAND ----------

import org.apache.spark.ml.feature.StringIndexer

val indexerPickup: org.apache.spark.ml.feature.StringIndexerModel = new StringIndexer()
                   .setInputCol("pickup_locn_id")
                   .setOutputCol("pickup_locn_id_indxd")
                   .fit(baseDF)
val indexerDropoff: org.apache.spark.ml.feature.StringIndexerModel = new StringIndexer()
                    .setInputCol("dropoff_locn_id")
                    .setOutputCol("dropoff_locn_id_indxd")
                    .fit(baseDF)
val indexerStoreFlag: org.apache.spark.ml.feature.StringIndexerModel =new StringIndexer()
                    .setInputCol("store_and_fwd_flag")
                    .setOutputCol("store_and_fwd_flag_indxd")
                    .fit(baseDF)
val indexerRatecode: org.apache.spark.ml.feature.StringIndexerModel = new StringIndexer()
                    .setInputCol("rate_code_id")
                    .setOutputCol("rate_code_id_indxd")
                    .fit(baseDF)

val indexedDF: org.apache.spark.sql.DataFrame =indexerPickup.transform(
                   indexerDropoff.transform(
                     indexerStoreFlag.transform(
                       indexerRatecode.transform(baseDF))))

// COMMAND ----------

//Before indexing
display(baseDF.select("pickup_locn_id","dropoff_locn_id","store_and_fwd_flag","rate_code_id"))

// COMMAND ----------

//After indexing
display(indexedDF.select("pickup_locn_id_indxd","dropoff_locn_id_indxd","store_and_fwd_flag_indxd","rate_code_id_indxd"))

// COMMAND ----------

// MAGIC %md #### 3.2. Encoding
// MAGIC 
// MAGIC One-hot encoding maps a categorical feature, represented as a label index, to a binary vector with at most a single one-value indicating the presence of a specific feature value from among the set of all feature values. This encoding allows algorithms which expect continuous features, such as Logistic Regression, to use categorical features. For string type input data, it is common to encode categorical features using StringIndexer first.

// COMMAND ----------

import org.apache.spark.ml.feature.OneHotEncoderEstimator

val encoder: org.apache.spark.ml.feature.OneHotEncoderModel = new OneHotEncoderEstimator()
  .setInputCols(Array("pickup_locn_id_indxd", "dropoff_locn_id_indxd","rate_code_id_indxd","store_and_fwd_flag_indxd"))
  .setOutputCols(Array("pickup_location_id_encd", "dropoff_location_id_encd","rate_code_id_encd","store_and_fwd_flag_encd"))
  .fit(indexedDF)

val encodedDF: org.apache.spark.sql.DataFrame = encoder.transform(indexedDF)

// COMMAND ----------

// MAGIC %md Let us display our transformed columns in `encodedData` to see how applying the one-hot encoder has affected our data.

// COMMAND ----------

display(encodedDF.select("pickup_location_id_encd", "dropoff_location_id_encd","rate_code_id_encd","store_and_fwd_flag_encd"))

// COMMAND ----------

// MAGIC %md #### 3.3. Assembling into vectors
// MAGIC 
// MAGIC VectorAssembler is a transformer that combines a given list of columns into a single vector column. It is useful for combining raw features and features generated by different feature transformers into a single feature vector, in order to train ML models like logistic regression and decision trees. VectorAssembler accepts the following input column types: all numeric types, boolean type, and vector type. In each row, the values of the input columns will be concatenated into a vector in the specified order.

// COMMAND ----------

import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vectors

val featureCols=Array("pickup_location_id_encd","trip_distance","pickup_hour","pickup_day","pickup_day_month","pickup_minute","pickup_weekday","pickup_month","rate_code_id_encd")
val assembler: org.apache.spark.ml.feature.VectorAssembler= new VectorAssembler()
                .setInputCols(featureCols)
                .setOutputCol("features")

val assembledDF = assembler.transform(encodedDF)
val assembledFinalDF = assembledDF.select("duration","features")

// COMMAND ----------

// MAGIC %md The vector assembler combines all our features into a single column of type Vector. Our dataframe `assembledFinalDF` will have our label(i.e. the column on which predictions have to be made) that is `duration_minutes` and the all the features in vector form called `features`

// COMMAND ----------

display(assembledFinalDF)

// COMMAND ----------

// MAGIC %md #### 3.4. Normalizing the vectors
// MAGIC 
// MAGIC Normalizer is a Transformer which transforms a dataset of Vector rows, normalizing each Vector to have unit norm. It takes parameter p, which specifies the p-norm used for normalization. (p=2 by default.) This normalization can help standardize your input data and improve the behavior of learning algorithms.

// COMMAND ----------

import org.apache.spark.ml.feature.Normalizer

val normalizedDF = new Normalizer()
  .setInputCol("features")
  .setOutputCol("normalizedFeatures")
  .transform(assembledFinalDF)

// COMMAND ----------

display(normalizedDF.select("duration","normalizedFeatures"))

// COMMAND ----------

// MAGIC %md #### 3.5. Indexing the label

// COMMAND ----------

import org.apache.spark.ml.feature.StringIndexer
val labelIndexer = new StringIndexer().setInputCol("duration").setOutputCol("duration_indxd_label")
val transformedDF = labelIndexer.fit(normalizedDF).transform(normalizedDF)

// COMMAND ----------

// MAGIC %md Let us view our final dataframe with the duration in the column `actuals` and transformed, normalized features as `normFeatures`

// COMMAND ----------

display(transformedDF.select("duration_indxd_label","normalizedFeatures"))

// COMMAND ----------

// MAGIC %md ### 4. Split data into training and test datasets
// MAGIC 
// MAGIC You're splitting the data randomly to generate two sets: one to use during training of the ML algorithm (training set), and the second to check whether the training is working (test set). This is widely done and a very good idea, as it catches overfitting which otherwise can make it seem like you have a great ML solution when it's actually effectively just memorized the answer for each data point and can't interpolate or generalize.

// COMMAND ----------

val Array(trainingDS, testDS) = transformedDF.randomSplit(Array(0.7, 0.3))

// COMMAND ----------

// MAGIC %md Since training machine learning models is an iterative process, it is a good idea to cache our final transformed data in-memory so that the Spark engine does not have to fetch it again and again from the source. This will greatly reduce the time taken for training the model and improve performance.
// MAGIC 
// MAGIC We will cache our training dataset and call a count action to trigger the caching.

// COMMAND ----------

trainingDS.cache
trainingDS.count

// COMMAND ----------

// MAGIC %md
// MAGIC ### 5. About Supervised Learning
// MAGIC 
// MAGIC The majority of practical machine learning uses supervised learning.
// MAGIC 
// MAGIC Supervised learning is where you have input variables (x) and an output variable (Y) and you use an algorithm to learn the mapping function from the input to the output.
// MAGIC 
// MAGIC Y = f(X)
// MAGIC 
// MAGIC The goal is to approximate the mapping function so well that when you have new input data (x) that you can predict the output variables (Y) for that data.
// MAGIC 
// MAGIC It is called supervised learning because the process of an algorithm learning from the training dataset can be thought of as a teacher supervising the learning process. We know the correct answers, the algorithm iteratively makes predictions on the training data and is corrected by the teacher. Learning stops when the algorithm achieves an acceptable level of performance.
// MAGIC 
// MAGIC Supervised learning problems can be further grouped into regression and classification problems.
// MAGIC 
// MAGIC - Classification: A classification problem is when the output variable is a category, such as “red” or “blue” or “disease” and “no disease”.
// MAGIC - Regression: A regression problem is when the output variable is a real value, such as “dollars” or “weight”.

// COMMAND ----------

// MAGIC %md
// MAGIC #### 5.1. About Linear Regression
// MAGIC Linear regression belongs to the family of regression algorithms. The goal of regression is to find relationships and dependencies between variables. It is modeling the relationship between a continuous scalar dependent variable y (also label or target in machine learning terminology) and one or more (a D-dimensional vector) explanatory variables (also independent variables, input variables, features, observed data, observations, attributes, dimensions, data point, ..) denoted X using a linear function. 
// MAGIC In regression analysis the goal is to predict a continuous target variable, whereas another area called classfication is predicting a label from a finite set. The model for a multiple regression which involves linear combination of input variables takes the form y = ß0 + ß1x1 + ß2x2 + ß3x3 + ..... + e . Linear regression belongs to a category of supervised learning algorithms. It means we train the model on a set of labeled data (training data) and then use the model to predict labels on unlabeled data (test data).
// MAGIC 
// MAGIC  The model (red line) is calculated using training data (blue points) where each point has a known label (y axis) to fit the points as accurately as possible by minimizing the value of a chosen loss function. We can then use the model to predict unknown labels (we only know x value and want to predict y value).
// MAGIC  
// MAGIC ![Linear Regression](http://upload.wikimedia.org/wikipedia/commons/thumb/3/3a/Linear_regression.svg/438px-Linear_regression.svg.png)

// COMMAND ----------

// MAGIC %md
// MAGIC #### 5.2. About Random Forest Regression
// MAGIC 
// MAGIC Random forests are ensembles of decision trees. Random forests combine many decision trees in order to reduce the risk of overfitting. The spark.ml implementation supports random forests for binary and multiclass classification and for regression, using both continuous and categorical features. Random Forest uses Decision Tree as the base model. Random Forests train each tree independently, using a random sample of the data. This randomness helps to make the model more robust than a single decision tree, and less likely to overfit on the training data.
// MAGIC 
// MAGIC For more information on the algorithm itself, please see the [spark.mllib documentation](https://spark.apache.org/docs/latest/mllib-ensembles.html#random-forests) on random forests.
// MAGIC ![Random Forest Algorithm](https://cdn-images-1.medium.com/max/1600/0*tG-IWcxL1jg7RkT0.png)

// COMMAND ----------

// MAGIC %md ### 6. Linear Regression Algorithm - model train and evaluate

// COMMAND ----------

// MAGIC %md
// MAGIC #### 6.1 Train and test model

// COMMAND ----------

import org.apache.spark.ml.regression.LinearRegression
// Create a LinearRegression instance. This instance is an Estimator.
val lr = new LinearRegression()
// We may set parameters using setter methods.
            .setLabelCol("duration_indxd_label")
            .setMaxIter(100)
// Print out the parameters, documentation, and any default values.
println(s"Linear Regression parameters:\n ${lr.explainParams()}\n")
// Learn a Linear Regression model. This uses the parameters stored in lr.
val lrModel = lr.fit(trainingDS)
// Make predictions on test data using the Transformer.transform() method.
// LinearRegression.transform will only use the 'features' column.
val lrPredictions = lrModel.transform(testDS)

// COMMAND ----------

// MAGIC %md Let us display a sample of 15 records of our predictions and the corresponding duration in Test data. We will order them by the difference between the actual and predicted values to see the predictions closest to the actual duration.

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
println("\nPredictions : " )
	lrPredictions.select($"duration_indxd_label".cast(IntegerType),$"prediction".cast(IntegerType)).orderBy(abs($"prediction"-$"duration_indxd_label")).distinct.show(15)

// COMMAND ----------

// MAGIC %md #### 6.2. Residual Analysis
// MAGIC 
// MAGIC Because a linear regression model is not always appropriate for the data, you should assess the appropriateness of the model by defining residuals and examining residual plots.
// MAGIC 
// MAGIC The difference between the observed value of the dependent variable (y) and the predicted value (ŷ) is called the residual (e). Each data point has one residual.
// MAGIC 
// MAGIC #####Residual = Observed value - Predicted value ##### 
// MAGIC 
// MAGIC A residual plot is a graph that shows the residuals on the vertical axis and the independent variable on the horizontal axis. If the points in a residual plot are randomly dispersed around the horizontal axis, a linear regression model is appropriate for the data; otherwise, a non-linear model is more appropriate.
// MAGIC 
// MAGIC You can checkout [this](https://en.wikipedia.org/wiki/Errors_and_residuals) Wikipedia link to get an in-depth idea of residuals

// COMMAND ----------

// MAGIC %md The Fitted vs Residuals plot is available for Linear Regression and Logistic Regression models. The Databricks’ Fitted vs Residuals plot is analogous to [R’s “Residuals vs Fitted” plots for linear models](http://stat.ethz.ch/R-manual/R-patched/library/stats/html/plot.lm.html).
// MAGIC 
// MAGIC Linear Regression computes a prediction as a weighted sum of the input variables. Using Databrick's inbuilt visualization capabilities, the Fitted vs Residuals plot can be used to assess a linear regression model’s goodness of fit.

// COMMAND ----------

display(lrModel, testDS, plotType="fittedVsResiduals")

// COMMAND ----------

// MAGIC %md In this scatter plot, the x-axis is the predicted values for the `duration` column made by our linear regression model, whereas the the y-axis shows the residual value,i.e. actual - predicted value for the predictions made by our linear regression model. In this graph, we can observe that the points are not dispersed randomly around the x-axis. We can infer from this that the linear model is not very appropriate for this data.

// COMMAND ----------

// MAGIC %md #### 6.3. Evaluation ##
// MAGIC spark.mllib comes with a number of machine learning algorithms that can be used to learn from and make predictions on data. When these algorithms are applied to build machine learning models, there is a need to evaluate the performance of the model on some criteria, which depends on the application and its requirements. spark.mllib also provides a suite of metrics for the purpose of evaluating the performance of machine learning models. Here we will use the `RegressionEvaluator` to evaluate our Linear Regression Model. RegressionEvaluator has many metrics for model evaluation including:
// MAGIC 
// MAGIC  - ##### Root Mean Squared Error (RMSE) #####
// MAGIC  - ##### Mean Squared Error (MSE) #####
// MAGIC  - ##### Mean absolute Error #####
// MAGIC  - ##### Unadjusted Coefficient of Determination #####

// COMMAND ----------

//Evaluate the results. Calculate accuracy and coefficient of determination R2.
import org.apache.spark.ml.evaluation.RegressionEvaluator
val evaluator_r2 = new RegressionEvaluator()
                .setPredictionCol("prediction")
                .setLabelCol("duration_indxd_label")//The metric we select for evaluation is the coefficient of determination
                .setMetricName("r2")

//As the name implies, isLargerBetter returns if a larger value is better or smaller for evaluation.
val isLargerBetter : Boolean = evaluator_r2.isLargerBetter
println("Coefficient of determination = " + evaluator_r2.evaluate(lrPredictions))


//Evaluate the results. Calculate Root Mean Square Error
val evaluator_rmse = new RegressionEvaluator()
                .setPredictionCol("prediction")
                .setLabelCol("duration_indxd_label")
//The metric we select for evaluation is RMSE
                .setMetricName("rmse")
//As the name implies, isLargerBetter returns if a larger value is better for evaluation.
val isLargerBetter1 : Boolean = evaluator_rmse.isLargerBetter
println("Root Mean Square Error = " + evaluator_rmse.evaluate(lrPredictions))

// COMMAND ----------

// MAGIC %md Using Linear Regression we get a Root Mean Square Error and  coefficient of determination as shown above. For R2,`isLargerBetter` returns  `true` meaning a a better model will have a larger coefficient of determination. For RMSE, the `isLargerBetter` returns `false` meaning a smaller value of RMSE is more desirable.
// MAGIC 
// MAGIC Spark ML provides us with multiple algorithms for classification that we can use train our model. This gives us flexibility to try multiple algorithms to see which gives us better results.
// MAGIC 
// MAGIC Let us now perform the same prediction using Random Forest Regression algorithm.

// COMMAND ----------

// MAGIC %md ### 7. Random Forest Regression Algorithm - model train and evaluate

// COMMAND ----------

// MAGIC %md
// MAGIC #### 7.1 Train and test model

// COMMAND ----------

import org.apache.spark.ml.regression.{RandomForestRegressionModel, RandomForestRegressor}
import org.apache.spark.ml.evaluation.RegressionEvaluator

//Create the model
val rf = new RandomForestRegressor()
                    .setLabelCol("duration_indxd_label")
                    .setFeaturesCol("features")
//Number of trees in the forest
                    .setNumTrees(100)
//Maximum depth of each tree in the forest
                    .setMaxDepth(15)

val rfModel = rf.fit(trainingDS)
	
//Predict on the test data
val rfPredictions = rfModel.transform(testDS)

// COMMAND ----------

// MAGIC %md Let us view our a sample of 15 records of the predictions made by Random Forest Model with the corresponding actual duration ordered by their difference. We can compare these results with the previous ones computed by Linear Regression.

// COMMAND ----------

println("\nPredictions :")
rfPredictions.select($"duration_indxd_label".cast(IntegerType),$"prediction".cast(IntegerType)).orderBy(abs($"duration_indxd_label"-$"prediction")).distinct.show(15)

// COMMAND ----------

// MAGIC %md Let us now evaluate our Random Forest Model to compare it with the previous model we trained using Linear Regression. We will calculate the same metric `R2` that is coefficient of determination and `RMSE` that is Root Mean Square Error for our Random Forest Model.

// COMMAND ----------

import org.apache.spark.ml.evaluation.RegressionEvaluator
val evaluator_r2 = new RegressionEvaluator()
               .setPredictionCol("prediction")
               .setLabelCol("duration_indxd_label")
               .setMetricName("r2")
println("Coefficient Of determination = " + evaluator_r2.evaluate(rfPredictions))

val evaluator_rmse = new RegressionEvaluator()
               .setPredictionCol("prediction")
               .setLabelCol("duration_indxd_label")
               .setMetricName("rmse")
println("RMSE = " + evaluator_rmse.evaluate(rfPredictions))

// COMMAND ----------

// MAGIC %md ### 7. Deciding on an algorithm based on performance - Linear Regression or Random Forest Regression?

// COMMAND ----------

// MAGIC %md Our model trained using Random Forest Algorithm gives us a Root Mean Square Error and coefficient of determination more desirable that what we got with Linear Regression. So, using Random Forest Algorithm for training has given us a better model to predict the time duration taken for each taxi to complete its trip.

// COMMAND ----------

// MAGIC %md ### 8. Persist model chosen
// MAGIC Since Apache Spark 2.0, we also have model persistence, i.e the ability to save and load models. Spark’s Machine Learning library MLlib include near-complete support for ML persistence in the DataFrame-based API. Key features of ML persistence include:
// MAGIC 
// MAGIC - Support for all language APIs in Spark: Scala, Java, Python & R
// MAGIC - Support for nearly all ML algorithms in the DataFrame-based API
// MAGIC - Support for single models and full Pipelines, both unfitted (a “recipe”) and fitted (a result)
// MAGIC - Distributed storage using an exchangeable format
// MAGIC 
// MAGIC We can save a single model that can be shared between workspaces, notebooks and languages. To demonstrate this, we will save our trained Random Forest Algorithm to Azure Blob Storage so we can access it in the future without having to train the model again.

// COMMAND ----------

//Destination directory to persist model
val modelDirectoryPath = "/mnt/workshop/consumption/nyctaxi/model/duration-pred/" 
//Delete any residual data from prior executions for an idempotent run
dbutils.fs.rm(destDataDirRoot,recurse=true)

// COMMAND ----------

//Persist model
rfModel.save(modelDirectoryPath)