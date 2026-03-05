from pyspark.sql import SparkSession

# Inicia a sessão do Spark
spark = SparkSession.builder \
    .master("local[2]") \
    .appName("TesteConexao") \
    .getOrCreate()

# Cria um mini DataFrame de teste
data = [("Genomics", 1), ("Data Engineering", 2), ("Spark", 3)]
df = spark.createDataFrame(data, ["Area", "ID"])

df.show()

# Fecha a sessão
spark.stop()