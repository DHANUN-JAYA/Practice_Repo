import pyspark
from pyspark.sql import SparkSession
# from pyspark.ml.feature import Imputer
spark=SparkSession.builder.appName('Practise').getOrCreate()
df_pyspark=spark.read.csv('C:/Users/Welcome/Desktop/test1.csv')
df_pyspark.show()
df_pyspark=spark.read.option('header','true').csv('C:/Users/Welcome/Desktop/test1.csv')
df_pyspark.show()
print(type(df_pyspark))
df_pyspark.select(['Name']).show()
print(df_pyspark['Name'])
print(df_pyspark.dtypes)
print(df_pyspark.describe().show())
#adding Colums in data frame
df_pyspark=df_pyspark.withColumn('Experience After',df_pyspark['Experience']+2)
df_pyspark.show()
#drop the column
df_pyspark=df_pyspark.drop('Experience After')
df_pyspark.show()
# Rename the column
df_pyspark.withColumnRenamed('Name','New Name').show()
#part 3
df_pyspark.drop('Name').show()
df_pyspark.na.drop().show()
# any-how
df_pyspark.na.drop(how='any').show()
#thershold
df_pyspark.na.drop(how='any',thresh=2).show()
df_pyspark.na.drop(how='any',subset=['Experience']).show()
#filling the missing values
df_pyspark.na.fill('Missing Value',['Experience','Age']).show()
# imputer=Imputer(inpuCols=['Age','Experience'])
# outputCols=["{}_imputed".format(c) for c in ['Age','Experience']].setStatergy('mean')
# imputer.fit(df_pyspark).transform(df_pyspark).show()
# Filter Options
#&
df_pyspark=df_pyspark.na.drop()
df_pyspark.show()
#filter Options
df_pyspark.filter("Experience>4").show()
df_pyspark.filter("Experience>4").select(['Name','age']).show()
df_pyspark.filter(df_pyspark['Experience']>4).show()
df_pyspark.filter((df_pyspark['Experience']>4) &  (df_pyspark['age']>30)).show()
df_pyspark.filter((df_pyspark['Experience']>4) |  (df_pyspark['age']>30)).show()
df_pyspark.filter((df_pyspark['Experience']>4) &  (df_pyspark['age']>30)).show()
#part 5
df_pyspark.printSchema()
# group by
df_pyspark.groupBy('name').sum().show()
