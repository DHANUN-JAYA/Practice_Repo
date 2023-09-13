import pyspark
from pyspark.sql import SparkSession

columns = ["language","users_count"]
data = [("Java", "20000"), ("Python", "100000"), ("Scala", "3000")]


spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
rdd = spark.sparkContext.parallelize(data)


dfFromRDD1 = rdd.toDF()
dfFromRDD1.printSchema()


columns = ["language","users_count"]
dfFromRDD1 = rdd.toDF(columns)
dfFromRDD1.printSchema()
dfFromRDD1.show()
#2.1 Using createDataFrame() from SparkSession

dfFromData2 = spark.createDataFrame(data).toDF(*columns)

#2.2 Using createDataFrame() with the Row type

# rowData = map(lambda x: Row(*x), data)
# dfFromData3 = spark.createDataFrame(rowData,columns)

#Create DataFrame with schema

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

data2 = [("James", "", "Smith", "36636", "M", 3000),
         ("Michael", "Rose", "", "40288", "M", 4000),
         ("Robert", "", "Williams", "42114", "M", 4000),
         ("Maria", "Anne", "Jones", "39192", "F", 4000),
         ("Jen", "Mary", "Brown", "", "F", -1)
         ]

schema = StructType([ \
    StructField("firstname", StringType(), True), \
    StructField("middlename", StringType(), True), \
    StructField("lastname", StringType(), True), \
    StructField("id", StringType(), True), \
    StructField("gender", StringType(), True), \
    StructField("salary", IntegerType(), True) \
    ])

df = spark.createDataFrame(data=data2, schema=schema)
df.printSchema()
df.show(truncate=False)

"""
df2 = spark.read.csv("/src/resources/file.csv")
df2 = spark.read.text("/src/resources/file.txt")
df2 = spark.read.json("/src/resources/file.json")
Avero, Parquet, ORC, Kafka
"""

dataDF = [(('James','','Smith'),'1991-04-01','M',3000),
  (('Michael','Rose',''),'2000-05-19','M',4000),
  (('Robert','','Williams'),'1978-09-05','M',4000),
  (('Maria','Anne','Jones'),'1967-12-01','F',4000),
  (('Jen','Mary','Brown'),'1980-02-17','F',-1)
]


from pyspark.sql.types import StructType,StructField, StringType, IntegerType
schema = StructType([
        StructField('name', StructType([
             StructField('firstname', StringType(), True),
             StructField('middlename', StringType(), True),
             StructField('lastname', StringType(), True)
             ])),
         StructField('dob', StringType(), True),
         StructField('gender', StringType(), True),
         StructField('gender', IntegerType(), True)
         ])
df = spark.createDataFrame(data = dataDF, schema = schema)
df.printSchema()
df.show()
df.withColumnRenamed("dob","DateOfBirth").printSchema()
#3. Using PySpark StructType – To rename a nested column in Dataframe
#from pyspark.sql.functions import *
# schema2 = StructType([
#     StructField("fname",StringType()),
#     StructField("middlename",StringType()),
#     StructField("lname",StringType())])
# df.select(col("name").cast(schema2), \
#      col("dob"), col("gender"),col("salary")) \
#    .printSchema()
#5. Using PySpark DataFrame withColumn – To rename nested columns
""""

from pyspark.sql.functions import *
df4 = df.withColumn("fname",col("name.firstname")) \
      .withColumn("mname",col("name.middlename")) \
      .withColumn("lname",col("name.lastname")) \
      .drop("name")
df4.printSchema()
"""

# 7. Using toDF() – To change all columns in a PySpark DataFrame

newColumns = ["newCol1","newCol2","newCol3","newCol4"]
df.toDF(*newColumns).printSchema()

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import *

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

dataDF = [(('James', '', 'Smith'), '1991-04-01', 'M', 3000),
          (('Michael', 'Rose', ''), '2000-05-19', 'M', 4000),
          (('Robert', '', 'Williams'), '1978-09-05', 'M', 4000),
          (('Maria', 'Anne', 'Jones'), '1967-12-01', 'F', 4000),
          (('Jen', 'Mary', 'Brown'), '1980-02-17', 'F', -1)
          ]

schema = StructType([
    StructField('name', StructType([
        StructField('firstname', StringType(), True),
        StructField('middlename', StringType(), True),
        StructField('lastname', StringType(), True)
    ])),
    StructField('dob', StringType(), True),
    StructField('gender', StringType(), True),
    StructField('salary', IntegerType(), True)
])

df = spark.createDataFrame(data=dataDF, schema=schema)
df.printSchema()

# Example 1
df.withColumnRenamed("dob", "DateOfBirth").printSchema()
# Example 2
df2 = df.withColumnRenamed("dob", "DateOfBirth") \
    .withColumnRenamed("salary", "salary_amount")
df2.printSchema()

# Example 3
schema2 = StructType([
    StructField("fname", StringType()),
    StructField("middlename", StringType()),
    StructField("lname", StringType())])

df.select(col("name").cast(schema2),
          col("dob"),
          col("gender"),
          col("salary")) \
    .printSchema()

# Example 4
df.select(col("name.firstname").alias("fname"),
          col("name.middlename").alias("mname"),
          col("name.lastname").alias("lname"),
          col("dob"), col("gender"), col("salary")) \
    .printSchema()

# Example 5
df4 = df.withColumn("fname", col("name.firstname")) \
    .withColumn("mname", col("name.middlename")) \
    .withColumn("lname", col("name.lastname")) \
    .drop("name")
df4.printSchema()

# Example 7
newColumns = ["newCol1", "newCol2", "newCol3", "newCol4"]
df.toDF(*newColumns).printSchema()

# Example 6
'''
not working
old_columns = Seq("dob","gender","salary","fname","mname","lname")
new_columns = Seq("DateOfBirth","Sex","salary","firstName","middleName","lastName")
columnsList = old_columns.zip(new_columns).map(f=>{col(f._1).as(f._2)})
df5 = df4.select(columnsList:_*)
df5.printSchema()
'''

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import *

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

dataDF = [(('James', '', 'Smith'), '1991-04-01', 'M', 3000),
          (('Michael', 'Rose', ''), '2000-05-19', 'M', 4000),
          (('Robert', '', 'Williams'), '1978-09-05', 'M', 4000),
          (('Maria', 'Anne', 'Jones'), '1967-12-01', 'F', 4000),
          (('Jen', 'Mary', 'Brown'), '1980-02-17', 'F', -1)
          ]

schema = StructType([
    StructField('name', StructType([
        StructField('firstname', StringType(), True),
        StructField('middlename', StringType(), True),
        StructField('lastname', StringType(), True)
    ])),
    StructField('dob', StringType(), True),
    StructField('gender', StringType(), True),
    StructField('salary', IntegerType(), True)
])

df = spark.createDataFrame(data=dataDF, schema=schema)
df.printSchema()

# Example 1
df.withColumnRenamed("dob", "DateOfBirth").printSchema()
# Example 2
df2 = df.withColumnRenamed("dob", "DateOfBirth") \
    .withColumnRenamed("salary", "salary_amount")
df2.printSchema()

# Example 3
schema2 = StructType([
    StructField("fname", StringType()),
    StructField("middlename", StringType()),
    StructField("lname", StringType())])

df.select(col("name").cast(schema2),
          col("dob"),
          col("gender"),
          col("salary")) \
    .printSchema()

# Example 4
df.select(col("name.firstname").alias("fname"),
          col("name.middlename").alias("mname"),
          col("name.lastname").alias("lname"),
          col("dob"), col("gender"), col("salary")) \
    .printSchema()

# Example 5
df4 = df.withColumn("fname", col("name.firstname")) \
    .withColumn("mname", col("name.middlename")) \
    .withColumn("lname", col("name.lastname")) \
    .drop("name")
df4.printSchema()

# Example 7
newColumns = ["newCol1", "newCol2", "newCol3", "newCol4"]
df.toDF(*newColumns).printSchema()

# Example 6
'''
not working
old_columns = Seq("dob","gender","salary","fname","mname","lname")
new_columns = Seq("DateOfBirth","Sex","salary","firstName","middleName","lastName")
columnsList = old_columns.zip(new_columns).map(f=>{col(f._1).as(f._2)})
df5 = df4.select(columnsList:_*)
df5.printSchema()
'''
#filters


from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import StringType, IntegerType, ArrayType

data = [
    (("James", "", "Smith"), ["Java", "Scala", "C++"], "OH", "M"),
    (("Anna", "Rose", ""), ["Spark", "Java", "C++"], "NY", "F"),
    (("Julia", "", "Williams"), ["CSharp", "VB"], "OH", "F"),
    (("Maria", "Anne", "Jones"), ["CSharp", "VB"], "NY", "M"),
    (("Jen", "Mary", "Brown"), ["CSharp", "VB"], "NY", "M"),
    (("Mike", "Mary", "Williams"), ["Python", "VB"], "OH", "M")
]

schema = StructType([
    StructField('name', StructType([
        StructField('firstname', StringType(), True),
        StructField('middlename', StringType(), True),
        StructField('lastname', StringType(), True)
    ])),
    StructField('languages', ArrayType(StringType()), True),
    StructField('state', StringType(), True),
    StructField('gender', StringType(), True)
])

df = spark.createDataFrame(data=data, schema=schema)
df.printSchema()
df.show(truncate=False)

# Using equals condition
df.filter(df.state == "OH").show(truncate=False)



# not equals condition
df.filter(df.state != "OH") \
    .show(truncate=False)
df.filter(~(df.state == "OH")) \
    .show(truncate=False)


#Using SQL col() function
from pyspark.sql.functions import col
df.filter(col("state") == "OH") \
    .show(truncate=False)


#Using SQL Expression
df.filter("gender == 'M'").show()
#For not equal
df.filter("gender != 'M'").show()
df.filter("gender <> 'M'").show()


data2 = [(2,"Michael Rose"),(3,"Robert Williams"),
     (4,"Rames Rose"),(5,"Rames rose")
  ]
df2 = spark.createDataFrame(data = data2, schema = ["id","name"])

# like - SQL LIKE pattern
df2.filter(df2.name.like("%rose%")).show()



#Filter multiple condition
df.filter( (df.state  == "OH") & (df.gender  == "M") ) \
    .show(truncate=False)

#Filter IS IN List values
li=["OH","CA","DE"]
df.filter(df.state.isin(li)).show()

# Using startswith
df.filter(df.state.startswith("N")).show()

#using endswith
df.filter(df.state.endswith("H")).show()

#contains
df.filter(df.state.contains("H")).show()

# rlike - SQL RLIKE pattern (LIKE with Regex)
#This check case insensitive
df2.filter(df2.name.rlike("(?i)^*rose$")).show()

# 8. Filter on an Array column

from pyspark.sql.functions import array_contains
df.filter(array_contains(df.languages,"Java")) \
    .show(truncate=False)

#Struct condition
df.filter(df.name.lastname == "Williams") \
    .show(truncate=False)

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType
from pyspark.sql.functions import col, array_contains

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

arrayStructureData = [
    (("James", "", "Smith"), ["Java", "Scala", "C++"], "OH", "M"),
    (("Anna", "Rose", ""), ["Spark", "Java", "C++"], "NY", "F"),
    (("Julia", "", "Williams"), ["CSharp", "VB"], "OH", "F"),
    (("Maria", "Anne", "Jones"), ["CSharp", "VB"], "NY", "M"),
    (("Jen", "Mary", "Brown"), ["CSharp", "VB"], "NY", "M"),
    (("Mike", "Mary", "Williams"), ["Python", "VB"], "OH", "M")
]

arrayStructureSchema = StructType([
    StructField('name', StructType([
        StructField('firstname', StringType(), True),
        StructField('middlename', StringType(), True),
        StructField('lastname', StringType(), True)
    ])),
    StructField('languages', ArrayType(StringType()), True),
    StructField('state', StringType(), True),
    StructField('gender', StringType(), True)
])

df = spark.createDataFrame(data=arrayStructureData, schema=arrayStructureSchema)
df.printSchema()
df.show(truncate=False)

df.filter(df.state == "OH") \
    .show(truncate=False)

df.filter(col("state") == "OH") \
    .show(truncate=False)

df.filter("gender  == 'M'") \
    .show(truncate=False)

df.filter((df.state == "OH") & (df.gender == "M")) \
    .show(truncate=False)

df.filter(array_contains(df.languages, "Java")) \
    .show(truncate=False)

df.filter(df.name.lastname == "Williams") \
    .show(truncate=False)
