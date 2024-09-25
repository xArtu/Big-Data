import math
from pyspark.sql import SparkSession

# Crear una SparkSession
spark = SparkSession.builder \
    .appName("KNN_Spark") \
    .getOrCreate()

def leerCSVConSpark(archivoCSV):
    # Leer el archivo CSV como un DataFrame de Spark
    df = spark.read.csv(archivoCSV, header=True, inferSchema=True)
    
    # Seleccionar solo las columnas necesarias (omitir 'id' y 'diagnosis')
    vectores = df.select(df.columns[2:]).rdd.map(lambda row: [float(x) for x in row])
    
    # Obtener el 'id' y 'diagnosis' por separado
    idsDiagnosis = df.select('id', 'diagnosis').rdd.map(lambda row: (row['id'], row['diagnosis'])).collect()
    
    return vectores.collect(), idsDiagnosis

def distanciaEuclidiana(v1, v2):
    return math.sqrt(sum((a - b) ** 2 for a, b in zip(v1, v2)))

def vectoresSimilares(vectores, idsDiagnosis, nuevoVector, n):
    distancias = []
    for i, vector in enumerate(vectores):
        distancia = distanciaEuclidiana(vector, nuevoVector)
        distancias.append((i, distancia))
    # Ordenar y regresar el n número más cercanos
    distancias.sort(key=lambda x: x[1])
    return [(idsDiagnosis[i][0], idsDiagnosis[i][1], distancias[j][1]) for j, (i, _) in enumerate(distancias[:n])]

# Archivo CSV
archivoCSV = 'data.csv'

# Leer vectores y ids con Spark
vectores, idsDiagnosis = leerCSVConSpark(archivoCSV)

# Nuevo vector
nuevoVector = [17.99,10.38,122.8,1001,0.1184,0.2776,0.3001,0.1471,0.2419,0.07871,1.095,0.9053,8.589,153.4,0.006399,0.04904,0.05373,0.01587,0.03003,0.006193,25.38,17.33,184.6,2019,0.1622,0.6656,0.7119,0.2654,0.4601,0.1189]

n = 100  # Rango de muestras a imprimir/comparar
vectores_similares = vectoresSimilares(vectores, idsDiagnosis, nuevoVector, n)

# Mostrar el n número más similar de vectores
for idVal, diagnosis, distancia in vectores_similares:
    print(f"ID: {idVal}, Diagnóstico: {diagnosis}, Distancia: {distancia:.4f}")
