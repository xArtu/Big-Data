from pyspark import SparkContext

# Crear un SparkContext
sc = SparkContext("local", "MiPrimeraAplicacionSpark")

# Crear un RDD a partir de una lista
data = [1, 2, 3, 4, 5]
rdd = sc.parallelize(data)

# Aplicar una transformación al RDD
squaredRDD = rdd.map(lambda x: x * x)

# Ejecutar una acción para obtener el resultado
result = squaredRDD.collect()
print(result)

# Detener el SparkContext
sc.stop()