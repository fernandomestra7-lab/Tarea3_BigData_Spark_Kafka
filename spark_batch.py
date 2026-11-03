from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count

# 1. Crear la sesión de Spark
spark = SparkSession.builder \
    .appName("TitanicBatchAnalysis") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Cargar los datos
print("--- 1. Cargando datos ---")
df = spark.read.csv("Titanic-Dataset.csv", header=True, inferSchema=True)

# 3. Limpieza de datos (Eliminamos filas donde falte la Edad o el Puerto de emba                                                                                                                                                             rque)
print("--- 2. Limpiando datos (quitando nulos en Age y Embarked) ---")
df_limpio = df.dropna(subset=["Age", "Embarked"])

# 4. Análisis Exploratorio (EDA)
print("--- 3. Mostrando los primeros 5 registros ---")
df_limpio.show(5)

print("--- 4. Resumen estadístico de Edad y Tarifa ---")
df_limpio.describe("Age", "Fare").show()

# 5. Análisis solicitado (Ejemplo de agrupación y promedios)
print("--- 5. Tasa de supervivencia por Sexo ---")
df_limpio.groupBy("Sex").agg(avg("Survived").alias("Tasa_Supervivencia")).show()

print("--- 6. Conteo de supervivientes por Clase (Pclass) ---")
df_limpio.groupBy("Pclass", "Survived").count().orderBy("Pclass").show()

# 6. Detener la sesión
spark.stop()
