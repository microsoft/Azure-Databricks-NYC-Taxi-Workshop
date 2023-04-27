# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks Delta - primer
# MAGIC
# MAGIC ### What's in this exercise?
# MAGIC In this exercise, we will complete the following in **batch** (streaming covered in the event hub primer):<br>
# MAGIC 1.  Create a dataset, persist in Delta format to DBFS backed by S3 Blob Storage, create a Delta table on top of the dataset<br>
# MAGIC 2.  Update one or two random records<br>
# MAGIC 3.  Delete one or two records<br>
# MAGIC 4.  Add a couple new columns to the data and understand considerations for schema evolution<br>
# MAGIC 4.  Discuss Databricks Delta's value proposition, and positioning in your big data architecture<br>
# MAGIC
# MAGIC References:
# MAGIC https://docs.azuredatabricks.net/delta/index.html<br>

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.0. Basic create operation

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1.0.1. Create some data

# COMMAND ----------

from libs.dbname import dbname
from libs.tblname import tblname, username
uname = username()

columns = ["book_id", "book_author", "book_name", "book_pub_year"]
vals = [
     ("b00001", "Arthur Conan Doyle", "A study in scarlet", 1887),
     ("b00001", "Arthur Conan Doyle", "A study in scarlet", 1887),
     ("b01001", "Arthur Conan Doyle", "The adventures of Sherlock Holmes", 1892),
     ("b00501", "Arthur Conan Doyle", "The memoirs of Sherlock Holmes", 1893),
     ("b00300", "Arthur Conan Doyle", "The hounds of Baskerville", 1901)
]
books_df = spark.createDataFrame(vals, columns)
books_df.printSchema
display(books_df)

# COMMAND ----------

# MAGIC  %md
# MAGIC  #### 1.0.2. Persist to Delta format

# COMMAND ----------

# Destination directory for Delta table
delta_table_directory = f"/mnt/workshop/users/{uname}/curated/books"
dbutils.fs.rm(delta_table_directory, recurse=True)

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists books_db.books;

# COMMAND ----------

# Persist dataframe to delta format without coalescing
books_df.write.format("delta").save(delta_table_directory)
spark.conf.set("nbvars.delta_table_directory", delta_table_directory)

# COMMAND ----------

# MAGIC  %md
# MAGIC  #### 1.0.3. Create Delta table

# COMMAND ----------

books_db = dblname(db="books")
spark.conf.set("nbvars.books_db", books_db)
books_tbl = tblname(db="books", tbl="books")
print("books_tbl:" + repr(books_tbl))
spark.conf.set("nbvars.books_tbl", books_tbl)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS ${nbvars.books_db};
# MAGIC
# MAGIC USE ${nbvars.books_db};
# MAGIC DROP TABLE IF EXISTS ${nbvars.books_tbl};
# MAGIC CREATE TABLE ${nbvars.books_tbl}
# MAGIC USING DELTA
# MAGIC LOCATION "${nbvars.delta_table_directory}";

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ${nbvars.books_tbl};

# COMMAND ----------

# MAGIC  %md
# MAGIC  #### 1.0.4. Performance optimization
# MAGIC  
# MAGIC  We will run the "OPTIMIZE" command to compact small files into larger for performance.
# MAGIC  Note: The performance improvements are evident at scale

# COMMAND ----------

# 1) Lets look at part file count, its 5
display(dbutils.fs.ls(delta_table_directory))

# COMMAND ----------

# 2) Lets read the dataset and check the partition size, it should be the same as number of small files
preDeltaOptimizeDF = spark.sql(f"select * from {books_tbl}")
preDeltaOptimizeDF.rdd.getNumPartitions()

# COMMAND ----------

# MAGIC %sql DESCRIBE DETAIL books_db.books;
# MAGIC --3) Lets run DESCRIBE DETAIL 
# MAGIC --Notice that numFiles = 5

# COMMAND ----------

# MAGIC %sql
# MAGIC --4) Now, lets run optimize
# MAGIC USE books_db;
# MAGIC OPTIMIZE books;

# COMMAND ----------

# MAGIC %sql DESCRIBE DETAIL books_db.books;
# MAGIC --5) Notice the number of files now - its 1 file

# COMMAND ----------

# 6) Lets look at the part file count, its 6 now!
# Guess why?
display(dbutils.fs.ls(delta_table_directory))

# COMMAND ----------

#7) Lets read the dataset and check the partition size, it should be the same as number of small files
postDeltaOptimizeDF = spark.sql(f"select * from {books_tbl}")
postDeltaOptimizeDF.rdd.getNumPartitions()
#Its 1, and not 6
#Guess why?

# COMMAND ----------

# MAGIC %sql 
# MAGIC --8a) Lets do some housekeeping now
# MAGIC set spark.databricks.delta.retentionDurationCheck.enabled = false

# COMMAND ----------

# MAGIC %sql
# MAGIC --8b) Run vacuum
# MAGIC VACUUM books_db.books retain 0 hours;

# COMMAND ----------

# 9) Lets look at the part file count, its 1 now!
display(dbutils.fs.ls(delta_table_directory))

# COMMAND ----------

# MAGIC %md
# MAGIC **Learnings:**<br>
# MAGIC OPTIMIZE compacts small files into larger ones but does not do housekeeping.<br>
# MAGIC VACUUM does the housekeeping of the small files from prior to compaction, and more.<br>

# COMMAND ----------

# MAGIC %md 
# MAGIC ## 2.0. Append operation

# COMMAND ----------

# MAGIC %md
# MAGIC 2.1. Create some data to add to the table & persist it

# COMMAND ----------

columns = ["book_id", "book_author", "book_name", "book_pub_year"]
vals = [
     ("b00909", "Arthur Conan Doyle", "A scandal in Bohemia", 1891),
     ("b00023", "Arthur Conan Doyle", "Playing with Fire", 1900)
]
books_df = spark.createDataFrame(vals, columns)
books_df.printSchema
display(books_df)

# COMMAND ----------

books_df.write.format("delta").mode("append").save(delta_table_directory)

# COMMAND ----------

# MAGIC %md
# MAGIC 2.2. Lets query the table without making any changes or running table refresh<br>
# MAGIC We should see the new book entries.

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from books_db.books;

# COMMAND ----------

# MAGIC %md
# MAGIC 2.3. Under the hood..

# COMMAND ----------

# MAGIC %sql DESCRIBE DETAIL books_db.books;

# COMMAND ----------

# MAGIC %md
# MAGIC 2.4. Optimize

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE books_db.books;
# MAGIC VACUUM books_db.books;

# COMMAND ----------

# MAGIC %md 
# MAGIC ## 3.0. Update/upsert operation

# COMMAND ----------

columns = ["book_id", "book_author", "book_name", "book_pub_year"]
vals = [
       ("b00001", "Sir Arthur Conan Doyle", "A study in scarlet", 1887),
       ("b00023", "Sir Arthur Conan Doyle", "A sign of four", 1890),
       ("b01001", "Sir Arthur Conan Doyle", "The adventures of Sherlock Holmes", 1892),
       ("b00501", "Sir Arthur Conan Doyle", "The memoirs of Sherlock Holmes", 1893),
       ("b00300", "Sir Arthur Conan Doyle", "The hounds of Baskerville", 1901),
       ("b09999", "Sir Arthur Conan Doyle", "The return of Sherlock Holmes", 1905),
       ("b00909", "Sir Arthur Conan Doyle", "A scandal in Bohemia", 1891),
       ("b00223", "Sir Arthur Conan Doyle", "Playing with Fire", 1900)
]
booksUpsertDF = spark.createDataFrame(vals, columns)
booksUpsertDF.printSchema
display(booksUpsertDF)

# COMMAND ----------

# 2) Create a temporary view on the upserts
booksUpsertDF.createOrReplaceTempView("books_upserts")

# COMMAND ----------

# MAGIC %sql
# MAGIC --3) Execute upsert
# MAGIC USE books_db;
# MAGIC
# MAGIC MERGE INTO books
# MAGIC USING books_upserts
# MAGIC ON books.book_id = books_upserts.book_id
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC     books.book_author = books_upserts.book_author,
# MAGIC     books.book_name = books_upserts.book_name,
# MAGIC     books.book_pub_year = books_upserts.book_pub_year
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (book_id, book_author, book_name, book_pub_year) VALUES (books_upserts.book_id, books_upserts.book_author, books_upserts.book_name, books_upserts.book_pub_year);

# COMMAND ----------

# MAGIC %sql
# MAGIC --4) Validate
# MAGIC Select * from books_db.books;

# COMMAND ----------

# 5) Files? How many?
display(dbutils.fs.ls(delta_table_directory))

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- 6) What does describe detail say?
# MAGIC DESCRIBE DETAIL books_db.books;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 7) Lets optimize 
# MAGIC OPTIMIZE books_db.books;
# MAGIC VACUUM books_db.books;

# COMMAND ----------

# MAGIC %md 
# MAGIC ## 4.0. Delete operation

# COMMAND ----------

# MAGIC %sql
# MAGIC --1) Lets isolate records to delete
# MAGIC select * from books_db.books where book_pub_year>=1900;

# COMMAND ----------

# MAGIC %sql
# MAGIC --2) Execute delete
# MAGIC USE books_db;
# MAGIC
# MAGIC DELETE FROM books where book_pub_year >= 1900;

# COMMAND ----------

# MAGIC %sql
# MAGIC --3) Lets validate
# MAGIC select * from books_db.books where book_pub_year>=1900;

# COMMAND ----------

# MAGIC %sql
# MAGIC --4) Lets validate further
# MAGIC select * from books_db.books;

# COMMAND ----------

# MAGIC %sql
# MAGIC --4) Lets validate further
# MAGIC select * from books_db.books;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6.0. Overwrite
# MAGIC Works only when there are no schema changes

# COMMAND ----------

columns = ["book_id", "book_author", "book_name", "book_pub_year"]
vals = [
     ("b00001", "Arthur Conan Doyle", "A study in scarlet", 1887),
     ("b00001", "Arthur Conan Doyle", "A study in scarlet", 1887),
     ("b01001", "Arthur Conan Doyle", "The adventures of Sherlock Holmes", 1892),
     ("b00501", "Arthur Conan Doyle", "The memoirs of Sherlock Holmes", 1893),
     ("b00300", "Arthur Conan Doyle", "The hounds of Baskerville", 1901),
     ("b00909", "Arthur Conan Doyle", "A scandal in Bohemia", 1891),
     ("b00023", "Arthur Conan Doyle", "Playing with Fire", 1900)
]
booksOverwriteDF = spark.createDataFrame(vals, columns)
booksOverwriteDF.printSchema
display(booksOverwriteDF)

# COMMAND ----------

# 2) Overwrite the table
booksOverwriteDF.write.format("delta").mode("overwrite").save(delta_table_directory)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 3) Query
# MAGIC -- Notice the "- the great" in the name in the couple records?
# MAGIC select * from books_db.books;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7.0. Automatic schema update

# COMMAND ----------

columns = ["book_id", "book_author", "book_name", "book_pub_year", "book_price"]
vals = [
     ("b00001", "Arthur Conan Doyle", "A study in scarlet", 1887, 2.33),
     ("b00001", "Arthur Conan Doyle", "A study in scarlet", 1887, 5.12),
     ("b01001", "Arthur Conan Doyle", "The adventures of Sherlock Holmes", 1892, 12.00),
     ("b00501", "Arthur Conan Doyle", "The memoirs of Sherlock Holmes", 1893, 13.39),
     ("b00300", "Arthur Conan Doyle", "The hounds of Baskerville", 1901, 22.00),
     ("b00909", "Arthur Conan Doyle", "A scandal in Bohemia", 1891, 18.00),
     ("b00023", "Arthur Conan Doyle", "Playing with Fire", 1900, 29.99)
]
booksNewColDF = spark.createDataFrame(vals, columns)
booksNewColDF.printSchema
display(booksNewColDF)

# COMMAND ----------

booksNewColDF.write.format("delta").option("mergeSchema", "true").mode("overwrite").save(delta_table_directory)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 3) Query
# MAGIC select * from books_db.books;

# COMMAND ----------

# MAGIC %sql
# MAGIC USE books_db;
# MAGIC
# MAGIC DELETE FROM books where book_price is null;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7.0. Partitioning
# MAGIC Works just the same as usual

# COMMAND ----------

columns = ["book_id", "book_author", "book_name", "book_pub_year", "book_price"]
vals = [
       ("b00001", "Sir Arthur Conan Doyle", "A study in scarlet", 1887, 5.33),
       ("b00023", "Sir Arthur Conan Doyle", "A sign of four", 1890, 6.00),
       ("b01001", "Sir Arthur Conan Doyle", "The adventures of Sherlock Holmes", 1892, 2.99),
       ("b00501", "Sir Arthur Conan Doyle", "The memoirs of Sherlock Holmes", 1893, 8.00),
       ("b00300", "Sir Arthur Conan Doyle", "The hounds of Baskerville", 1901, 4.00),
       ("b09999", "Sir Arthur Conan Doyle", "The return of Sherlock Holmes", 1905, 2.22),
       ("c09999", "Jules Verne", "Journey to the Center of the Earth ", 1864, 2.22),
        ("d09933", "Jules Verne", "The return of Sherlock Holmes", 1870, 3.33),
        ("f09945", "Jules Verne", "Around the World in Eighty Days", 1873, 4.44)
]
books_partitioned_df = spark.createDataFrame(vals, columns)
books_partitioned_df.printSchema
display(books_partitioned_df)

# COMMAND ----------

# 2) Persist
books_part_path = f"/mnt/workshop/users/{uname}/curated/delta/books-part"

dbutils.fs.rm(books_part_path, recurse=True)
books_partitioned_df.write.format("delta").partitionBy("book_author").save(books_part_path)

books_tbl_part = tblname(db="books", tbl="books_part")
print("books_tbl_part:" + repr(books_tbl_part))
spark.conf.set("nbvars.books_tbl_part", books_tbl_part)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 3) Create table
# MAGIC USE ${nbvars.books_db};
# MAGIC DROP TABLE IF EXISTS ${nbvars.books_tbl_part};
# MAGIC CREATE TABLE ${nbvars.books_tbl_part}
# MAGIC USING DELTA
# MAGIC LOCATION "${nbvars.books_part_path}";

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 4) Seamless
# MAGIC select * from ${nbvars.books_tbl_part};

# COMMAND ----------

# 5) Is it really partitioned?
display(dbutils.fs.ls(books_part_path))

# COMMAND ----------

# MAGIC %md
# MAGIC Visit the portal and take a look at the storage account to see how its organized.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8.0. History

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY ${nbvars.books_tbl};

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


