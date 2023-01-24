from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('parquetFile').getOrCreate()
df=spark.read.csv('shop_sales.csv',header=True,inferSchema=True)
uniqe_invoice_date =[data[0] for data in df.dropDuplicates(["InvoiceDate"]).select("InvoiceDate").collect()]
for date in uniqe_invoice_date:
    filter_data = df.filter(df.InvoiceDate == date)
    file_name = "parq_data/{}.parquert".format(date.strftime('%m_%d_%Y'))
    filter_data.write.mode("overwrite").parquet(file_name)