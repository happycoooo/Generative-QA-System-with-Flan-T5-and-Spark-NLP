import json
import sparknlp
from sparknlp.base import DocumentAssembler
from sparknlp.annotator import T5Transformer
from pyspark.ml import Pipeline
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructType, StructField, ArrayType, IntegerType
from pyspark.sql.functions import col, from_json, pandas_udf
from pyspark.sql.functions import PandasUDFType
import pandas as pd

# Initialize Spark session with Spark NLP and Kafka configuration
spark = SparkSession.builder \
    .appName("Spark NLP") \
    .master("local[*]") \
    .config("spark.driver.memory", "16G") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.kryoserializer.buffer.max", "2000M") \
    .config("spark.driver.maxResultSize", "0") \
    .config("spark.jars.packages", 
            "com.johnsnowlabs.nlp:spark-nlp_2.12:5.3.3,"
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
            "org.apache.kafka:kafka-clients:3.5.0") \
    .config("spark.executorEnv.PYSPARK_PYTHON", "/root/anaconda3/bin/python") \
    .getOrCreate()

# Define schema for Kafka messages
schema = StructType([
    StructField("id", StringType(), True),
    StructField("title", StringType(), True),
    StructField("context", StringType(), True),
    StructField("question", StringType(), True),
    StructField("answers", StructType([
        StructField("text", ArrayType(StringType()), True),
        StructField("answer_start", ArrayType(IntegerType()), True)
    ]), True),
    StructField("timestamp", StringType(), True)
])

# Read the stream from Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "q3") \
    .option("startingOffsets", "earliest") \
    .load() \
    .selectExpr("CAST(value AS STRING) as raw_data")

# Parse JSON data
parsed_df = df.select(from_json(col("raw_data"), schema).alias("data")).select("data.*")


@pandas_udf(ArrayType(StringType()), PandasUDFType.SCALAR)
def predict_udf(context_series: pd.Series, question_series: pd.Series) -> pd.Series:
    # Prepare Text data
    document_assembler = DocumentAssembler() \
        .setInputCol("context") \
        .setOutputCol("document")

    # Load a pre-trained T5Transformer model
    t5 = T5Transformer.load("/data/lab/assignments/proj2/project2/spark_nlp") \
        .setTask("question: ") \
        .setInputCols(["document"]) \
        .setOutputCol("answer")

    # Create a pipeline with the document assembler and T5 transformer
    pipeline = Pipeline().setStages([document_assembler, t5])
    model = pipeline.fit(spark.createDataFrame([("", "")], schema=["context", "question"]))

    # Combine the input series into a DataFrame
    input_df = pd.DataFrame({'context': context_series, 'question': question_series})

    # Convert the Pandas DataFrame to a Spark DataFrame
    spark_input_df = spark.createDataFrame(input_df)

    # Apply the pipeline model to the Spark DataFrame
    result_df = model.transform(spark_input_df)

    # Collect the results and extract the 'answer.result' column
    results = result_df.select("answer.result").rdd.flatMap(lambda x: x).collect()

    # Return the results as a Pandas Series
    return pd.Series(results)

# Apply the UDF to the streaming DataFrame
processed_df = parsed_df.withColumn("predictions", predict_udf(col("context"), col("question")))

# Write the result to the console
query = processed_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()