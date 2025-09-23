import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, to_json, struct, col
from kafka import KafkaProducer
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, StringType
import spacy
import json

bootstrap_servers = ['localhost:9092']
producer = KafkaProducer(bootstrap_servers=bootstrap_servers, value_serializer=lambda v: v.encode('utf-8'))

nlp = spacy.load("en_core_web_sm")

@udf(ArrayType(StringType()))
def spacy_ner_and_extract_words(text):
    doc = nlp(text)
    return [ent.text for ent in doc.ents]

def process_words(df, epoch_id):
    word_count = dict(df.take(10)) 
    producer.send('Result', value=json.dumps(word_count).encode())

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("""
        Usage: Q2_2.py <bootstrap-servers> <subscribe-type> <topics>
        """, file=sys.stderr)
        sys.exit(-1)

    bootstrapServers = sys.argv[1]
    subscribeType = sys.argv[2]
    topics = sys.argv[3]

    spark = SparkSession.builder.appName("WordCount").getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    lines = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", bootstrapServers) \
        .option(subscribeType, topics) \
        .option("failOnDataLoss", "false") \
        .load() \
        .selectExpr("CAST(value AS STRING) as data")
    
    spark.udf.register("spacy_ner_and_extract_words", spacy_ner_and_extract_words)

    word_Counts = lines.select("data", explode(spacy_ner_and_extract_words("data")).alias("word")).groupBy("word").count().orderBy(col("count").desc())

    query = word_Counts.writeStream \
    .outputMode("complete") \
    .foreachBatch(process_words) \
    .start()

    query.awaitTermination()