from pyspark.sql import SparkSession
from pyspark.sql.functions import min,max,lit
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number

spark = SparkSession.builder.appName('parquetFile').getOrCreate()
df=spark.read.csv('shop_sales.csv',header=True,inferSchema=True)
uniqe_invoice_no =[data[0] for data in df.dropDuplicates(["InvoiceNo"]).select("InvoiceNo").collect()]
drop_coloms_name = ("StockCode","Description","Quantity","InvoiceDate","UnitPrice","CustomerID","Country")

w3 = Window.partitionBy("InvoiceNo").orderBy(col("UnitPrice").desc())

for no in uniqe_invoice_no:
    filter_data = df.filter(df.InvoiceNo == no)
    min_value = filter_data.select(min('UnitPrice')).collect()[0][0]
    max_value = filter_data.select(max('UnitPrice')).collect()[0][0]
    filter_data = filter_data.withColumn("min_UnitPrice", lit(min_value))
    filter_data = filter_data.withColumn("max_UnitPrice", lit(max_value))
    data=filter_data.withColumn("row",row_number().over(w3)).filter(col("row") == 1).drop("row")
    final_data=data.drop(*drop_coloms_name)
    #final_data.show()
    file_name = "min_max_data/{}.parquert".format(no)
    filter_data.write.mode("overwrite").parquet(file_name)
