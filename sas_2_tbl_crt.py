from pyspark.sql import SparkSession
import sys
import re

fl=sys.argv[1]
mr_fl=re.sub(r"^",r"maprfs:",fl)
tbl_nm=sys.argv[2]

spark = SparkSession.builder.master("yarn").appName("sas_2_tbl_crt").enableHiveSupport().getOrCreate()
df=spark.read.format('com.github.saurfang.sas.spark').load(mr_fl)
df.write.format('orc').saveAsTable(tbl_nm)