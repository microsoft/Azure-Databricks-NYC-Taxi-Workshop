// Databricks notebook source
// MAGIC %md
// MAGIC # Databricks Delta - primer
// MAGIC 
// MAGIC ### What's in this exercise?
// MAGIC In this exercise, we will complete the following in **batch** (streaming covered in the event hub primer):<br>
// MAGIC 1.  Create a dataset, persist in Delta format to DBFS backed by Azure Blob Storage, create a Delta table on top of the dataset<br>
// MAGIC 2.  Update one or two random records<br>
// MAGIC 3.  Delete one or two records<br>
// MAGIC 4.  Add a couple new columns to the data and understand considerations for schema evolution<br>
// MAGIC 4.  Discuss Databricks Delta's value proposition, and positioning in your big data architecture<br>
// MAGIC 
// MAGIC References:
// MAGIC https://docs.azuredatabricks.net/delta/index.html<br>

// COMMAND ----------

spark.conf.set("spark.databricks.delta.preview.enabled", true) 

// COMMAND ----------

// MAGIC %md 
// MAGIC ## 1.0. Basic create operation

// COMMAND ----------

// MAGIC %md
// MAGIC #### 1.0.1. Create some data

// COMMAND ----------

val booksDF = Seq(
   ("b00001", "Arthur Conan Doyle", "A study in scarlet", 1887),
   ("b00023", "Arthur Conan Doyle", "A sign of four", 1890),
   ("b01001", "Arthur Conan Doyle", "The adventures of Sherlock Holmes", 1892),
   ("b00501", "Arthur Conan Doyle", "The memoirs of Sherlock Holmes", 1893),
   ("b00300", "Arthur Conan Doyle", "The hounds of Baskerville", 1901)
).toDF("book_id", "book_author", "book_name", "book_pub_year")

booksDF.printSchema
booksDF.show

// COMMAND ----------

//Lets look at partition size - its 5
booksDF.rdd.partitions.size

// COMMAND ----------

// MAGIC  %md
// MAGIC  #### 1.0.2. Persist to Delta format

// COMMAND ----------

//Destination directory for Delta table
val deltaTableDirectory = "/mnt/workshop/curated/books"
dbutils.fs.rm(deltaTableDirectory, recurse=true)

//Persist dataframe to delta format without coalescing
booksDF.write.format("delta").save(deltaTableDirectory)

// COMMAND ----------

// MAGIC  %md
// MAGIC  #### 1.0.3. Create Delta table

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE DATABASE IF NOT EXISTS books_db;
// MAGIC 
// MAGIC USE books_db;
// MAGIC DROP TABLE IF EXISTS books;
// MAGIC CREATE TABLE books
// MAGIC USING DELTA
// MAGIC LOCATION "/mnt/workshop/curated/books";

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from books_db.books;

// COMMAND ----------

// MAGIC  %md
// MAGIC  #### 1.0.4. Performance optimization
// MAGIC  We will run the "OPTIMIZE" command to compact small files into larger for performance.
// MAGIC  Note: The performance improvements are evident at scale

// COMMAND ----------

//1) Lets look at part file count, its 5
display(dbutils.fs.ls("/mnt/workshop/curated/books"))

// COMMAND ----------

//2) Lets read the dataset and check the partition size, it should be the same as number of small files
val preDeltaOptimizeDF = spark.sql("select * from books_db.books")
preDeltaOptimizeDF.rdd.partitions.size

// COMMAND ----------

// MAGIC %sql DESCRIBE DETAIL books_db.books;
// MAGIC --3) Lets run DESCRIBE DETAIL 
// MAGIC --Notice that numFiles = 5

// COMMAND ----------

// MAGIC %sql
// MAGIC --4) Now, lets run optimize
// MAGIC USE books_db;
// MAGIC OPTIMIZE books;

// COMMAND ----------

// MAGIC %sql DESCRIBE DETAIL books_db.books;
// MAGIC --5) Notice the number of files now - its 1 file

// COMMAND ----------

//6) Lets look at the part file count, its 6 now!
//Guess why?
display(dbutils.fs.ls("/mnt/workshop/curated/books"))

// COMMAND ----------

//7) Lets read the dataset and check the partition size, it should be the same as number of small files
val postDeltaOptimizeDF = spark.sql("select * from books_db.books")
postDeltaOptimizeDF.rdd.partitions.size
//Its 1, and not 6
//Guess why?

// COMMAND ----------

// MAGIC %sql 
// MAGIC --8a) Lets do some housekeeping now
// MAGIC set spark.databricks.delta.retentionDurationCheck.enabled = false

// COMMAND ----------

// MAGIC %sql
// MAGIC --8b) Run vacuum
// MAGIC VACUUM books_db.books retain 0 hours;

// COMMAND ----------

//9) Lets look at the part file count, its 1 now!
display(dbutils.fs.ls("/mnt/workshop/curated/books"))

// COMMAND ----------

// MAGIC %md
// MAGIC **Learnings:**<br>
// MAGIC OPTIMIZE compacts small files into larger ones but does not do housekeeping.<br>
// MAGIC VACUUM does the housekeeping of the small files from prior to compaction, and more.<br>

// COMMAND ----------

// MAGIC %md 
// MAGIC ## 2.0. Append operation

// COMMAND ----------

// MAGIC %md
// MAGIC 2.1. Create some data to add to the table & persist it

// COMMAND ----------

val booksDF = Seq(
   ("b00909", "Arthur Conan Doyle", "A scandal in Bohemia", 1891),
   ("b00023", "Arthur Conan Doyle", "Playing with Fire", 1900)
).toDF("book_id", "book_author", "book_name", "book_pub_year")

booksDF.printSchema
booksDF.show

// COMMAND ----------

booksDF.write.format("delta").mode("append").save(deltaTableDirectory)

// COMMAND ----------

// MAGIC %md
// MAGIC 2.2. Lets query the table without making any changes or running table refresh<br>
// MAGIC We should see the new book entries.

// COMMAND ----------

// MAGIC %sql
// MAGIC Select * from books_db.books;

// COMMAND ----------

// MAGIC %md
// MAGIC 2.3. Under the hood..

// COMMAND ----------

// MAGIC %sql DESCRIBE DETAIL books_db.books;

// COMMAND ----------

// MAGIC %md
// MAGIC 2.4. Optimize

// COMMAND ----------

// MAGIC %sql
// MAGIC OPTIMIZE books_db.books;
// MAGIC VACUUM books_db.books;

// COMMAND ----------

// MAGIC %md 
// MAGIC ## 3.0. Update/upsert operation

// COMMAND ----------

// (1) Update: Changing author name to include prefix of "Sir", and (2) Insert: adding a new book
val booksUpsertDF = Seq(
                         ("b00001", "Sir Arthur Conan Doyle", "A study in scarlet", 1887),
                         ("b00023", "Sir Arthur Conan Doyle", "A sign of four", 1890),
                         ("b01001", "Sir Arthur Conan Doyle", "The adventures of Sherlock Holmes", 1892),
                         ("b00501", "Sir Arthur Conan Doyle", "The memoirs of Sherlock Holmes", 1893),
                         ("b00300", "Sir Arthur Conan Doyle", "The hounds of Baskerville", 1901),
                         ("b09999", "Sir Arthur Conan Doyle", "The return of Sherlock Holmes", 1905),
                         ("b00909", "Sir Arthur Conan Doyle", "A scandal in Bohemia", 1891),
                         ("b00223", "Sir Arthur Conan Doyle", "Playing with Fire", 1900)
                        ).toDF("book_id", "book_author", "book_name", "book_pub_year")
booksUpsertDF.show()

// COMMAND ----------

// 2) Create a temporary view on the upserts
booksUpsertDF.createOrReplaceTempView("books_upserts")

// COMMAND ----------

// MAGIC %sql
// MAGIC --3) Execute upsert
// MAGIC USE books_db;
// MAGIC 
// MAGIC MERGE INTO books
// MAGIC USING books_upserts
// MAGIC ON books.book_id = books_upserts.book_id
// MAGIC WHEN MATCHED THEN
// MAGIC   UPDATE SET
// MAGIC     books.book_author = books_upserts.book_author,
// MAGIC     books.book_name = books_upserts.book_name,
// MAGIC     books.book_pub_year = books_upserts.book_pub_year
// MAGIC WHEN NOT MATCHED
// MAGIC   THEN INSERT (book_id, book_author, book_name, book_pub_year) VALUES (books_upserts.book_id, books_upserts.book_author, books_upserts.book_name, books_upserts.book_pub_year);

// COMMAND ----------

// MAGIC %sql
// MAGIC --4) Validate
// MAGIC Select * from books_db.books;

// COMMAND ----------

// 5) Files? How many?
display(dbutils.fs.ls("/mnt/workshop/curated/books"))

// COMMAND ----------

// MAGIC %sql 
// MAGIC -- 6) What does describe detail say?
// MAGIC DESCRIBE DETAIL books_db.books;

// COMMAND ----------

// MAGIC %sql
// MAGIC -- 7) Lets optimize 
// MAGIC OPTIMIZE books_db.books;
// MAGIC VACUUM books_db.books;

// COMMAND ----------

// MAGIC %md 
// MAGIC ## 4.0. Delete operation

// COMMAND ----------

// MAGIC %sql
// MAGIC --1) Lets isolate records to delete
// MAGIC select * from books_db.books where book_pub_year>=1900;

// COMMAND ----------

// MAGIC %sql
// MAGIC --2) Execute delete
// MAGIC USE books_db;
// MAGIC 
// MAGIC DELETE FROM books where book_pub_year >= 1900;

// COMMAND ----------

// MAGIC %sql
// MAGIC --3) Lets validate
// MAGIC select * from books_db.books where book_pub_year>=1900;

// COMMAND ----------

// MAGIC %sql
// MAGIC --4) Lets validate further
// MAGIC select * from books_db.books;

// COMMAND ----------

// MAGIC %md
// MAGIC ## 5.0. Schema updates
// MAGIC Databricks Delta lets you update the schema of a table. The following types of changes are supported:<br>
// MAGIC 
// MAGIC Adding new columns (at arbitrary positions)<br>
// MAGIC Reordering existing columns<br>
// MAGIC You can make these changes explicitly using DDL or implicitly using DML.<br>
// MAGIC 
// MAGIC https://docs.azuredatabricks.net/delta/delta-batch.html#write-to-a-table

// COMMAND ----------

// MAGIC %md
// MAGIC ## 6.0. Overwrite
// MAGIC Works only when there are no schema changes

// COMMAND ----------

// 1) Lets create a dataset
val booksOverwriteDF = Seq(
                         ("b00001", "Sir Arthur Conan Doyle - the great", "A study in scarlet", 1887),
                         ("b00023", "Sir Arthur Conan Doyle", "A sign of four", 1890),
                         ("b01001", "Sir Arthur Conan Doyle", "The adventures of Sherlock Holmes", 1892),
                         ("b00501", "Sir Arthur Conan Doyle", "The memoirs of Sherlock Holmes", 1893),
                         ("b00300", "Sir Arthur Conan Doyle", "The hounds of Baskerville", 1901),
                         ("b09999", "Sir Arthur Conan Doyle", "The return of Sherlock Holmes", 1905),
                         ("b00909", "Sir Arthur Conan Doyle", "A scandal in Bohemia", 1891),
                         ("b00223", "Sir Arthur Conan Doyle - the great", "Playing with Fire", 1900)
                        ).toDF("book_id", "book_author", "book_name", "book_pub_year")
booksOverwriteDF.show()

// COMMAND ----------

// 2) Overwrite the table
booksOverwriteDF.write.format("delta").mode("overwrite").save("/mnt/workshop/curated/books")

// COMMAND ----------

// MAGIC %sql
// MAGIC -- 3) Query
// MAGIC -- Notice the "- the great" in the name in the couple records?
// MAGIC select * from books_db.books;

// COMMAND ----------

// MAGIC %md
// MAGIC ## 6.0. Automatic schema update

// COMMAND ----------

// 1) Create dataset with *new* price column
val booksNewColDF = Seq(
                         ("b00001", "Sir Arthur Conan Doyle", "A study in scarlet", 1887, 5.33),
                         ("b00023", "Sir Arthur Conan Doyle", "A sign of four", 1890, 6.00),
                         ("b01001", "Sir Arthur Conan Doyle", "The adventures of Sherlock Holmes", 1892, 2.99),
                         ("b00501", "Sir Arthur Conan Doyle", "The memoirs of Sherlock Holmes", 1893, 8.00),
                         ("b00300", "Sir Arthur Conan Doyle", "The hounds of Baskerville", 1901, 4.00),
                         ("b09999", "Sir Arthur Conan Doyle", "The return of Sherlock Holmes", 1905, 2.22)
                        ).toDF("book_id", "book_author", "book_name", "book_pub_year","book_price")
booksNewColDF.show()

// COMMAND ----------

booksNewColDF.write
  .format("delta")
  .option("mergeSchema", "true")
  .mode("overwrite")
  .save("/mnt/workshop/curated/books")

// COMMAND ----------

// MAGIC %sql
// MAGIC -- 3) Query
// MAGIC select * from books_db.books;

// COMMAND ----------

// MAGIC %md
// MAGIC ## 7.0. Partitioning
// MAGIC Works just the same as usual

// COMMAND ----------

// 1) Sample data - two authors this time - we will partition by author
val booksPartitionedDF = Seq(
                         ("b00001", "Sir Arthur Conan Doyle", "A study in scarlet", 1887, 5.33),
                         ("b00023", "Sir Arthur Conan Doyle", "A sign of four", 1890, 6.00),
                         ("b01001", "Sir Arthur Conan Doyle", "The adventures of Sherlock Holmes", 1892, 2.99),
                         ("b00501", "Sir Arthur Conan Doyle", "The memoirs of Sherlock Holmes", 1893, 8.00),
                         ("b00300", "Sir Arthur Conan Doyle", "The hounds of Baskerville", 1901, 4.00),
                         ("b09999", "Sir Arthur Conan Doyle", "The return of Sherlock Holmes", 1905, 2.22),
                         ("c09999", "Jules Verne", "Journey to the Center of the Earth ", 1864, 2.22),
                          ("d09933", "Jules Verne", "The return of Sherlock Holmes", 1870, 3.33),
                          ("f09945", "Jules Verne", "Around the World in Eighty Days", 1873, 4.44)
                        ).toDF("book_id", "book_author", "book_name", "book_pub_year","book_price")
booksPartitionedDF.show()

// COMMAND ----------

// 2) Persist
dbutils.fs.rm("/mnt/workshop/curated/delta/books-part", recurse=true)

booksPartitionedDF.write
  .format("delta")
  .partitionBy("book_author")
  .save("/mnt/workshop/curated/delta/books-part")

// COMMAND ----------

// MAGIC %sql
// MAGIC -- 3) Create table
// MAGIC USE books_db;
// MAGIC DROP TABLE IF EXISTS books_part;
// MAGIC CREATE TABLE books_part
// MAGIC USING DELTA
// MAGIC LOCATION "/mnt/workshop/curated/delta/books-part";

// COMMAND ----------

// MAGIC %sql
// MAGIC -- 4) Seamless
// MAGIC select * from books_db.books_part;

// COMMAND ----------

// 5) Is it really partitioned?
display(dbutils.fs.ls("/mnt/workshop/curated/delta/books-part"))

// COMMAND ----------

// MAGIC %md
// MAGIC Visit the portal and take a look at the storage account to see how its organized.

// COMMAND ----------

// MAGIC %md
// MAGIC ## 8.0. History

// COMMAND ----------

// MAGIC %sql
// MAGIC DESCRIBE HISTORY books_db.books;