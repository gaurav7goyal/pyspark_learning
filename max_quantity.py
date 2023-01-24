from pyspark.sql import SparkSession
import pyspark.sql.functions as F
spark = SparkSession.builder.appName('parquetFile').getOrCreate()
emp_RDD = spark.sparkContext.emptyRDD()
df=spark.read.csv('shop_sales.csv',header=True,inferSchema=True)
df1 = df.withColumn('year',F.year(df.InvoiceDate))
final_df = spark.createDataFrame([], df1.schema)
final_df.show()

uniqe_year =[data[0].strftime('%Y') for data in df.dropDuplicates(["InvoiceDate"]).select("InvoiceDate").collect()]
#print(set(uniqe_year))
for yr in set(uniqe_year):
    #filter_data = df.filter(df.InvoiceNo.strftime('%Y') == yr)
    df2=df1.filter(df1.year == yr) 
    max_quantity = df2.select(F.max('Quantity')).collect()[0][0]
    df3 = df2.filter(df2.Quantity == max_quantity)
    output=final_df.union(df3)
    final_df = output
file_name =  "maximum_Quantity_per_yr.csv"
final_df.toPandas().to_csv(file_name, encoding='utf-8')
