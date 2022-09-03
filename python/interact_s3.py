import boto3
import pandas as pd 

# Criar um cliente para interagir com o AWS S3

s3_client = boto3.client('s3')

s3_client.download_file('datalake-cin-edc-2022', 'stellaacc - stellaacc.csv', 'stellaacc - stellaacc.csv')

#df = pd.read_csv('stellaacc - stellaacc.csv', sep=";")
#print(df)

s3_client.upload_file("mx-segment.csv", "datalake-cin-edc-2022", "data/mx-segment.csv")

# não precisa colocar a pasta root do pc quando se referir aos arquivos locais.

# da aula do EMR
# import pyspark

#ler os dados do enem
#enem = {
 #   spark.read
  #  .format("csv")
   # .option("header", True)
    #.option("inferSchema", True)
    #.option("delimiter", ";")
    #.load("datalake-cin-edc-2022/raw-data")
#}

# enem.printSchema()

#df = (
#    enem 
#    .select("NU_IDADE", "TP_SEXO", "TP_ESTADO_CIVIL", "TP_COR_RACA", "TP_NACIONALIDADE"
#            "NU_NOTA_CN", "NU_NOTA_CH", "NU_NOTA_LC", "NU_NOTA_MT")
#)

#df.show(10)
#df.count()
# from pyspark.sql.functions import mean, max, min, col, count

#(
#    df
#    .groupBy("TP_SEXO")
#    .agg(
#        mean(col("NU_IDADE")).alias("MED_IDADE"),
#        count(col("TP_SEXO")).alias("CONTAGEM"),
#        max(col("NU_NOTA_MT")).alias("MAX_NOTA_MT"),
#        min(col("NU_NOTA_MT")).alias("MIN_NOTA_MT"),
#    )
#    .show()
#)

######## FORMATO PARQUET #########
#(
#    enem
#    .write
#    .mode("overwrite")
#    .format("parquet")
#    .partitionBy("year")
#    .save("s3://datalake-cin-edc-2022/staging/enem")
#)

#spark.stop()

#abre um arquivo novo em .py
#from pyspark.sql.functions import mean, max, min, col, count
#from pyspark.sql import SparkSession

#spark = (
#    SparkSession.builder.appName("ExerciseSpark")
#    .getOrCreate()
#)

#ler os dados do enem
#enem = {
 #   spark.read
  #  .format("csv")
   # .option("header", True)
    #.option("inferSchema", True)
    #.option("delimiter", ";")
    #.load("datalake-cin-edc-2022/raw-data")
#}

#(
#    enem
#    .write
#    .mode("overwrite")
#    .format("parquet")
#    .partitionBy("year")
#    .save("s3://datalake-cin-edc-2022/staging/enem")
#)

# baixa o .py e fazer upload no lake separado por pastas
# salvou como job_emr.py

# cluster -> step ->  JAR ->command-runner.jar, arguments-> spark-submit --master yarn --deploy-mode cluster s3://path do job_spark.py

#lendo os novos dados escritos em parquet
#enem_parquet = (
#    spark
#    .read
#    .format("parquet")
#    .load("s3://path")
#)