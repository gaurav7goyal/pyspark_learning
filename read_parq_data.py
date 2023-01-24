import os
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('parquetFile').getOrCreate()
for file in os.listdir('parq_data'):
    file_path='parq_data/'+file
    parDF=spark.read.parquet(file_path)
    # parDF.createOrReplaceTempView("ParquetTable")
    # custdf = spark.sql("select * from ParquetTable where CustomerID IS NOT NULL ")
    # print(parDF)
    without_custID_df = parDF.filter(parDF.CustomerID.isNull())
    print('no custmer id',without_custID_df.count())


    with_custID_df = parDF.filter(parDF.CustomerID.isNotNull())
    print('custmer id',with_custID_df.count())
    #if parDF.count() > 0:
        #cust_data_file_name = 'cust_id_parq_data/{}'.format(file)
        #custdf.write.mode("overwrite").parquet(cust_data_file_name)
        