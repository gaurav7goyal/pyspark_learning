
from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local").appName("test").getOrCreate()
Access_key_ID="AKIAWCIIN"
Secret_access_key="+5mk2ORIs6uZtE0daBotr0fttZx"
# Enable hadoop s3a settings
spark.sparkContext._jsc.hadoopConfiguration().set("com.amazonaws.services.s3.enableV4", "true")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider", \
                                     "com.amazonaws.auth.InstanceProfileCredentialsProvider,com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.AbstractFileSystem.s3a.impl", "org.apache.hadoop.fs.s3a.S3A")

spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key",Access_key_ID)
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key",Secret_access_key)
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.ap-south-1.amazonaws.com")

data="s3a://shop-sale-project-bucket/shop_sales.csv"
df = spark.read.csv('s3a://shop-sale-project-bucket/shop_sales.csv', header=True,inferSchema=True)
uniqe_invoice_date =[data[0] for data in df.dropDuplicates(["InvoiceDate"]).select("InvoiceDate").collect()]
for date in uniqe_invoice_date:
    df.filter(df.InvoiceDate == date)
    file_name = "s3a://shop-sale-project-bucket/parq_data/{}.parquert".format(date.strftime('%m_%d_%Y'))
    df.write.mode("overwrite").parquet(file_name)
   
