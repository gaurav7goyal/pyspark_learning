from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('parquetFile').getOrCreate()
df=spark.read.csv('shop_sales.csv',header=True,inferSchema=True)
df.createOrReplaceTempView("df_view")
uniqe_invoice_date =[data[0] for data in df.dropDuplicates(["InvoiceDate"]).select("InvoiceDate").collect()]
for date in uniqe_invoice_date:
    df = spark.sql(
        """
        SELECT * 
        FROM df_view
        WHERE InvoiceDate == '{}'""".format(date)
    )
    file_name = "parq_data/{}.parquert".format(date.strftime('%m_%d_%Y'))
    df.write.mode("overwrite").parquet(file_name)