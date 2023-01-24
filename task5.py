from pyspark.sql import SparkSession
from pyspark.sql.functions import min,max,lit


spark = SparkSession.builder.appName('parquetFile').getOrCreate()
df=spark.read.csv('shop_sales.csv',header=True,inferSchema=True)
uniqe_invoice_no =[data[0] for data in df.dropDuplicates(["InvoiceNo"]).select("InvoiceNo").collect()]
for no in uniqe_invoice_no:
    filter_data = df.filter(df.InvoiceNo == no)
    min_value = filter_data.select(min('UnitPrice')).collect()[0][0]
    max_value = filter_data.select(max('UnitPrice')).collect()[0][0]
    filter_data = filter_data.withColumn("min_UnitPrice", lit(min_value))
    filter_data = filter_data.withColumn("max_UnitPrice", lit(max_value))
    file_name = "min_max_data/{}.parquert".format(no)
    filter_data.write.mode("overwrite").parquet(file_name)