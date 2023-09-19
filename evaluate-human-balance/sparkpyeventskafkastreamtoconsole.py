from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, unbase64, base64, split
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, ArrayType, DateType

app_name = 'events-stream-to-console-stedi-spark'

spark = SparkSession.builder.appName(app_name).getOrCreate()
spark.sparkContext.setLogLevel('WARN')

stedi_events_raw_stream_df = spark \
    .readStream \
    .format('kafka') \
    .option('kafka.bootstrap.servers', 'localhost:9092') \
    .option('subscribe', 'stedi-events') \
    .option('startingOffsets', 'earliest') \
    .load()

stedi_events_stream_df = stedi_events_raw_stream_df.selectExpr('cast(value as string) value')

customer_kafka_schema = StructType(
    [
        StructField('customer', StringType()),
        StructField('score', StringType()),
        StructField('riskDate', DateType())
    ]
)

stedi_events_stream_df.withColumn('value', from_json('value', customer_kafka_schema)).select(col('value.*')).createOrReplaceTempView('CustomerRisk')

stedi_events_select = spark.sql('select * from CustomerRisk')
stedi_events_select.writeStream \
    .outputMode('append') \
    .format('console') \
    .start() \
    .awaitTermination()
