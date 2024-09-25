from pyspark.sql import SparkSession
sesion = SparkSession.builder.appName("Leer CSV").getOrCreate()
df=sesion.read.csv("/home/arturo/Big Data/06-session/data.csv", header=True, inferSchema=True)
df.show()

for columna in df.columns:
    print("Datos de columna")
    columna_rdd = df.select(columna).rdd
    valores_rdd = df.select(columna).rdd.map(lambda row: row[0])
    sumatoria = valores_rdd.reduce(lambda x,y: x+y)
    print(sumatoria)