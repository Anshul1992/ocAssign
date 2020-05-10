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

def solution1(spark):
    """
    :param spark:
    :type spark:
    :return:
    :rtype:
    """
    # schema = StructType([
    #     StructField('id',IntegerType(),True),
    #     StructField('event_name', StringType(), True),
    #     StructField('people_count', IntegerType(), True),
    # ])
    # df = spark.read.format('csv').options(header='true').schema(schema).load("/Users/anshgoel/Desktop/oneChampionshipAssignment/Input/event.csv")


    df = spark.read.format('csv').options(inferschema="true",header='true').load(
        "/Users/anshgoel/Desktop/ocAssign/Input/event.csv")
    df.show()
    df.printSchema()

    df.createOrReplaceTempView('sql')

    df_final = spark.sql("""SELECT DISTINCT series1.*
                    FROM sql series1
                    cross JOIN sql series2
                    cross JOIN sql series3
                    ON ((series1.id = series2.id - 1 AND series1.id = series3.id -2)
                    OR (series3.id = series1.id - 1 AND series3.id = series2.id -2)
                    OR (series3.id = series2.id - 1 AND series3.id = series1.id -2))
                    WHERE series1.people_count >= 100
                    AND series2.people_count >= 100
                    AND series3.people_count >= 100""")

    df_final.coalesce(1).write.mode('overwrite').csv("/Users/anshgoel/Desktop/ocAssign/Output/solution1.csv")

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

    df.write.mode('overwrite').json("/Users/anshgoel/Desktop/ocAssign/Output/solution2.json")

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
    solution1(spark)
    solution2(spark)
