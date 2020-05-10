##########################################################################################
#
# To execute: (provide spark submit configuration as per the cluster config)
# nohup spark-submit deploy-mode= __  --queue adhoc --executor-memory __ --num-executors __  main.py > main.log
#
##########################################################################################


from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import col
import json
from fuzzywuzzy import fuzz

def solution2(spark):
    """
    :param spark:
    :type spark:
    :return:
    :rtype:
    """
    config = json.loads(open("/Users/anshgoel/Desktop/ocAssign/Input/schema.json").read())
    print(config)
    cr_schema = []
    properties = config['properties']
    for k, v in properties.items():
        k1 = "\"" + k + "\""
        if properties[k]['type'] == 'string':
            cr_schema.append("StructField(" + "\"" + k + "\"" + ", StringType(), True)")
            # print(a)

        if properties[k]['type'] == 'integer':
            cr_schema.append("StructField(" + "\"" + k + "\"" + ", IntegerType(), True)")
            # print(a)
            # StructField("name", StringType(), True)

    # print(str(cr_schema))
    cr_schema_rep = str(cr_schema).replace("'", "")
    print(cr_schema_rep)
    cr_schema_rep = eval(cr_schema_rep)
    print(cr_schema_rep)
    schema = StructType(cr_schema_rep)
    print(schema)

    df = spark.read.format('csv').options(header='true').schema(schema).load(
        "/Users/anshgoel/Desktop/ocAssign/Input/data.csv")
    df.show()
    df.printSchema()
    # df.where(col('datetime')=='10/1/15 8:02').show()

    df.write.mode('overwrite').json("/Users/anshgoel/Desktop/ocAssign/Output/question2.json")

    # df = spark.read.format('csv').options(header='true', inferSchema='true').load("/Users/anshgoel/Desktop/ocAssign/Input/data.csv")
    # df.show()
    # df.printSchema()

    # for k,v in config['properties'].items():
    #     for i in df.columns:
    #         # print(str(fuzz.partial_ratio(k, i))+"--->"+k+" json with filename "+i)
    #         if fuzz.partial_ratio(k, i)>65:
    #             print(k)

    # for i in range(0,len(df.columns)):
    #     if (i==0):
    #         schema = "StructType([ StructField(\"name\", StringType(), True),"
    #     print(i)

    # for i in df.columns:
    #     b = "\""+i+"\""
    #     print(b)
    #     try:
    #
    #         if config['properties'][b]:
    #             print("yoo")
    #         print(config['properties'][b])
    #     except:
    #         print(i + " column not exist in schema")
    #
    #
    # schema = StructType([
    #     StructField("name", StringType(), True),
    #     StructField("age", IntegerType(), True)])
    #
    # print(schema)


if __name__ == "__main__":
    spark = SparkSession.builder.appName("ocAssign").getOrCreate()
    solution2(spark)
