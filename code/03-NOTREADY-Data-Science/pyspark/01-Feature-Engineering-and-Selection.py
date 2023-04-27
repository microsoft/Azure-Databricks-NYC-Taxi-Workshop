# Databricks notebook source
# MAGIC %run "./99-Shared-Functions-and-Settings"

# COMMAND ----------

from pyspark.sql.functions import *
from datetime import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Data Selection
# MAGIC Since we want to predict the duration of the trip based on the pickup and dropoff locations - we'll first need to make sure our data is relevent and representative of the information we'll have at the time of prediction.

# COMMAND ----------

# Read in data from the materialized view
tripData = spark.read.table('taxi_db.taxi_trips_mat_view')

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.1. `pickup_location_id` and `dropoff_location_id`
# MAGIC First, as we examined the data, it appears that pickup_location_id and dropoff_location_id are fields that were added at some point in our data... let's look at trips that have the location by month.

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1.1.1 Verify date available

# COMMAND ----------

display(tripData.groupBy('pickup_year', 'pickup_month')
                .agg(sum((col('pickup_location_id')!=0).astype('int')).alias('trips_with_location'))
                .orderBy('pickup_year', 'pickup_month'))

# COMMAND ----------

# MAGIC %md
# MAGIC Let's verify with another look - specifically at setting a cutoff of July 1st, 2016 at 12:00 AM. We'll also count the number of trips AFTER that data with no location ID.
# MAGIC 
# MAGIC The chart below shows us that, indeed, the locatoin field was added after that date. Therefore, we'll filter out only those days on or after July 1st.

# COMMAND ----------

cutoff_date = datetime.strptime('2016-07-01', '%Y-%m-%d')

crosstabData = (tripData.withColumn('pickup_after_cutoff', col("pickup_datetime") >= cutoff_date) # Create new boolean column if pickup_datetime >
                .withColumn('location_present', col("pickup_location_id") != 0)
                .crosstab('pickup_after_cutoff', 'location_present').toPandas())

# We'll generate a graph to compare - you can find the function to generate this graph in the notebook 00-Shared-Functions
display(generate_crosstab(crosstabData))

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC One thing to note here - matplotlib plots can be displayed inline (without using the Jupyter magic command `%matplotlib inline`)
# MAGIC 
# MAGIC Instead, we need to run the `display` function on the figure item returned by matplotlib. In the [Simple Plot sample on the Matplotlib site](https://matplotlib.org/gallery/lines_bars_and_markers/simple_plot.html), we generate a figure object with the `fig, ax = plt.subplots()` command
# MAGIC 
# MAGIC ```python
# MAGIC import matplotlib
# MAGIC import matplotlib.pyplot as plt
# MAGIC import numpy as np
# MAGIC # Data for plotting
# MAGIC t = np.arange(0.0, 2.0, 0.01)
# MAGIC s = 1 + np.sin(2 * np.pi * t)
# MAGIC 
# MAGIC fig, ax = plt.subplots()
# MAGIC ax.plot(t, s)
# MAGIC 
# MAGIC ax.set(xlabel='time (s)', ylabel='voltage (mV)',
# MAGIC        title='About as simple as it gets, folks')
# MAGIC ax.grid()
# MAGIC ```
# MAGIC 
# MAGIC In Databricks, we then run `display(fig)` to show that graph inline.

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1.1.2 Filter data to after July 1st, 2016
# MAGIC We'll filter out all trips prior to out cutoff date.

# COMMAND ----------

# We'll write over the Trip Data
tripData = tripData.filter(col("pickup_datetime") >= cutoff_date)

# And we'll verify that no "unknown" pickup locations are still in the data
fullRowCount = tripData.count()

goodRowCount = tripData.filter(col('pickup_location_id') == 0).count()

print('{0} rows with no pickup_location_id out of {1:,} total rows in dataset'.format(goodRowCount, fullRowCount))
assert(goodRowCount == 0) # Using an assert statement to raise an error if other rows slip through

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.2. What Data is Available at the time of booking?
# MAGIC 
# MAGIC Next, we want to avoid any 'data leakage' - specifically where we include data we know now for past values that we won't know at the time of predictions in the future.
# MAGIC 
# MAGIC Let's look again at the columns and a sample of the data in our dataset.

# COMMAND ----------

tripData.printSchema()

# COMMAND ----------

display(tripData)

# COMMAND ----------

# MAGIC %md
# MAGIC In looking at this, there are several features that we won't know at the time of booking. For instance, we won't know the fare (since it's partially a function of duration).
# MAGIC 
# MAGIC We also won't know anything about the trip dropoff time. (However, we'll keep in the dropoff_datetime 0- since we'll need that to compute duration)
# MAGIC 
# MAGIC In all, of the 51 columns in our dataset, here are the columns we won't have access to at the time of booking (for our example, we'll assume we have the dropoff location, since the driver needs to know where they are driving to):
# MAGIC * `dropoff_datetime`
# MAGIC * `fare_amount`
# MAGIC * `extra`
# MAGIC * `mta_tax`
# MAGIC * `tip_amount`
# MAGIC * `tolls_amount`
# MAGIC * `ehail_fee`
# MAGIC * `improvement_surcharge`
# MAGIC * `total_amount`
# MAGIC * `payment_type`
# MAGIC * `payment_type_description`
# MAGIC * `dropoff_year`
# MAGIC * `dropoff_month`
# MAGIC * `dropoff_day`
# MAGIC * `dropoff_hour`
# MAGIC * `dropoff_minute`
# MAGIC * `dropoff_second`
# MAGIC 
# MAGIC Let's drop these columns and see what's left.

# COMMAND ----------

columns_to_drop = {'fare_amount','extra','mta_tax','tip_amount','tolls_amount',
                   'ehail_fee','improvement_surcharge','total_amount','payment_type',
                   'payment_type_description','dropoff_year','dropoff_month',
                   'dropoff_hour', 'dropoff_day','dropoff_minute','dropoff_second'}

tripData = tripData.select([column for column in tripData.columns if column not in columns_to_drop])

# COMMAND ----------

# display(tripData.describe())

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.3. Which columns are blank and which columns are derived from other columns?
# MAGIC 
# MAGIC We don't want a lot of redundant data entering our model - because the effects of a particular feature can be amplified. For example - in our dataset we have the `pickup_datetime`, the `pickup_year`, the `trip_year`, etc. We'll want to be careful here not to represent the same thing multiple times. In this instance, we'll keep `pickup_year`, `pickup_month`, `pickup_day` and `pickup_hour`. I'll assume that the minute and seconds are too granular to effect the trip_duration, but we may need to revisit later.
# MAGIC 
# MAGIC Similarily, the latitude and longitude values of pickup and dropoff are pseudo-encoded in the location_id. We'll drop these to avoid redundancy.
# MAGIC 
# MAGIC With `pickup_location_id` and `dropoff_location_id`, we've included the lookup values here. Therefore, we'll exclude the IDs, because individual values of borough, etc. may have value there.
# MAGIC 
# MAGIC In all, we'll drop:
# MAGIC * `vendor_id`  - Encoded in `vendor_abbreviation`
# MAGIC * `pickup_datetime` - Will be dropped after duration calculation
# MAGIC * `dropoff_datetime` - Will be dropped after duration calculation
# MAGIC * `rate_code_id` - Encoded in `rate_code_description`, etc.
# MAGIC * `pickup_location_id` - Encoded in `pickup_borough`, etc.
# MAGIC * `dropoff_location_id` - Encoded in `dropoff_borough`, etc.
# MAGIC * `pickup_longitude` - Blank
# MAGIC * `pickup_latitude` - Blank
# MAGIC * `dropoff_longitude` - Blank
# MAGIC * `dropoff_latitude` - Blank
# MAGIC * `vendor_description` - Encoded in `vendor_abbreviation`
# MAGIC * `trip_type_description` - Mostly blank
# MAGIC * `month_name_short` - Encoded in `pickup_year`, etc.
# MAGIC * `month_name_full` - Encoded in `pickup_year`, etc.
# MAGIC * `pickup_minute` - Too granular
# MAGIC * `pickup_second` - Too granular
# MAGIC * `trip_year` - Encoded in `pickup_year`, etc.
# MAGIC * `trip_month` - Encoded in `pickup_year`, etc.

# COMMAND ----------

columns_to_drop = columns_to_drop.union({'vendor_id', 'rate_code_id', 'pickup_location_id', 'dropoff_location_id', 
                                         'pickup_longitude', 'pickup_latitude', 'dropoff_longitude', 'dropoff_latitude', 
                                         'trip_type', 'vendor_description', 'trip_type_description', 'month_name_short', 
                                         'month_name_full', 'pickup_minute', 'pickup_second', 'trip_year', 'trip_month'})

tripData = tripData.select([column for column in tripData.columns if column not in columns_to_drop])

# COMMAND ----------

display(tripData)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Feature (and Label) Engineering

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1 `is_weekend_flag` and `is_rush_hour_flag` columns

# COMMAND ----------

tripData = (tripData.withColumn('is_weekend_flag', date_format(col('pickup_datetime'), 'E').isin(['Sat', 'Sun']).astype('int'))
                    .withColumn('is_rush_hour_flag', ((col('is_weekend_flag')==0) & ((col('pickup_hour').between(7, 10)) | (col('pickup_hour').between(16, 19)))).astype('int')))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 `duration` column
# MAGIC 
# MAGIC We've been asked to predict "duration", but we don't have that represented anywhere. Therefore, we'll need to "engineer" that feature (or label in this instance)
# MAGIC 
# MAGIC We'll use the `unix_timestamp` function in PySpark to convert our timestamps to a UNIX Timestamp (# of seconds since January 1st, 1970 at 12:00AM UTC) and then subtract to get durations in seconds.

# COMMAND ----------

tripData = tripData.withColumn('duration_minutes', round((unix_timestamp(col('dropoff_datetime'))-unix_timestamp(col('pickup_datetime')))/60, 2))

columns_to_drop = columns_to_drop.union({'pickup_datetime', 'dropoff_datetime'})
tripData = tripData.select([column for column in tripData.columns if column not in columns_to_drop])

display(tripData)

# COMMAND ----------

display(tripData.describe(['duration_minutes']))

# COMMAND ----------

# We'll filter out any negative trip or trip over 2 hours...

tripData = tripData.where(col('duration_minutes').between(0, 120))
display(tripData.describe(['duration_minutes']))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.3 Other feature engineering?
# MAGIC There may be other features that we could engineer here, but for this lab we'll skip other feature engineering / exploration

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Moving to another notebook
# MAGIC Now, we'll take the dataframe we created here and "transfer" it to another notebook. We can do that through the SparkSQL API by registering a temporary view. 

# COMMAND ----------

tripData.createOrReplaceGlobalTempView(model_dataset_name)

# You can cache the temporary view for increase performance - but it might not be optimal in a workshop setting
# spark.catalog.cacheTable('global_temp.{0}'.format(model_dataset_name))
display(spark.read.table('global_temp.{0}'.format(model_dataset_name)))

# COMMAND ----------

