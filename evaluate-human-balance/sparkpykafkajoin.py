from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, unbase64, base64, split, expr
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, ArrayType, DateType


redis_schema = StructType(
    [
        StructField('key', StringType()),
        StructField('existType', StringType()),
        StructField('Ch', StringType()),
        StructField('Incr', BooleanType()),
        StructField('zSetEntries', ArrayType(
            StructType([
                StructField('element', StringType()),
                StructField('score', StringType())
            ])
        ))
    ]
)

kafka_customer_json_schema = StructType(
    [
        StructField('customerName', StringType()),
        StructField('email', StringType()),
        StructField('phone', StringType()),
        StructField('birthDay', DateType()),
    ]
)

customer_kafka_schema = StructType(
    [
        StructField('customer', StringType()),
        StructField('score', StringType()),
        StructField('riskDate', DateType())
    ]
)

app_name = 'stedi-risk-graph'
spark = SparkSession.builder.appName(app_name).getOrCreate()
spark.sparkContext.setLogLevel('WARN')

stedi_redis_events_raw_stream_df = spark \
    .readStream \
    .format('kafka') \
    .option('kafka.bootstrap.servers', 'localhost:9092') \
    .option('subscribe', 'redis-server') \
    .option('startingOffsets', 'earliest') \
    .load()

stedi_redis_events_stream_df = stedi_redis_events_raw_stream_df.selectExpr('cast(value as string) value')


stedi_redis_events_stream_df.withColumn('value', from_json('value', redis_schema)).select(col('value.*')).createOrReplaceTempView('RedisSortedSet')
z_entries_stedi_encoded_df = spark.sql('select zSetEntries[0].element as encodedCustomer from RedisSortedSet')
z_entries_stedi_decoded_df = z_entries_stedi_encoded_df.withColumn('customer', unbase64(z_entries_stedi_encoded_df.encodedCustomer).cast('string'))
z_entries_stedi_decoded_df.withColumn('customer', from_json('customer', kafka_customer_json_schema))\
                          .select(col('customer.*')) \
                          .createOrReplaceTempView('CustomerRecords')


email_and_birth_day_stream_df = spark.sql('select * from CustomerRecords where email is not null and birthDay is not null')
email_and_birth_day_stream_df = email_and_birth_day_stream_df.withColumn('birthYear', split(email_and_birth_day_stream_df.birthDay, '-').getItem(0))
email_and_birth_day_stream_df = email_and_birth_day_stream_df.select(col('email'), col('birthYear'))


stedi_events_raw_stream_df = spark \
    .readStream \
    .format('kafka') \
    .option('kafka.bootstrap.servers', 'localhost:9092') \
    .option('subscribe', 'stedi-events') \
    .option('startingOffsets', 'earliest') \
    .load()

stedi_events_stream_df = stedi_events_raw_stream_df.selectExpr('cast(value as string) value')


stedi_events_stream_df.withColumn('value', from_json('value', customer_kafka_schema)).select(col('value.*')).createOrReplaceTempView('CustomerRisk')

stedi_events_risk_select_df = spark.sql('select customer,score from CustomerRisk')


stedi_score_stream_join_select_df = stedi_events_risk_select_df.join(email_and_birth_day_stream_df, expr('customer = email')) \
    .selectExpr('cast(customer as string) as key', 'to_json(struct(*)) as value')

stedi_score_stream_join_select_df.writeStream \
    .format('kafka') \
    .option('kafka.bootstrap.servers', 'localhost:9092') \
    .option('topic', 'stedi-risk-topic') \
    .option('checkpointLocation', '/tmp/checkpoint') \
    .start() \
    .awaitTermination()
