import os
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('parquetFile').getOrCreate()
for file in os.listdir('parq_data'):
    file_path='parq_data/'+file
    parDF=spark.read.parquet(file_path)
    without_custID_df = parDF.filter(parDF.CustomerID.isNull())
    if without_custID_df.count() > 0:
        withoutcust_data_file_name = 'without_custId_data/{}'.format(file)
        without_custID_df.write.mode("overwrite").parquet(withoutcust_data_file_name)
    
    with_custID_df = parDF.filter(parDF.CustomerID.isNotNull())
    if with_custID_df.count() > 0:
        cust_data_file_name = 'cust_id_data/{}'.format(file)
        without_custID_df.write.mode("overwrite").parquet(cust_data_file_name)

        
        