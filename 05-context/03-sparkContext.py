from pyspark import SparkContext
import math

sc = SparkContext("local", "tercerContext")

vectorIngrediente = ["uva", "haba", "nuez", "naranja"]
vectorDulzura = [8, 3, 3, 7]
vectorCrunch = [5, 7, 6, 3]
vectorTipo = ["fruta", "vegetal", "proteina", "fruta"]
vectorDulzuraTomate = [6, 6, 6, 6]
vectorCrunchTomate = [4, 4, 4, 4]

# Creación de los RDDs
rddDulzura = sc.parallelize(vectorDulzura)
rddCrunch = sc.parallelize(vectorCrunch)
rddIngrediente = sc.parallelize(vectorIngrediente)
rddTipo = sc.parallelize(vectorTipo)
rddDulzuraTomate = sc.parallelize(vectorDulzuraTomate)
rddCrunchTomate = sc.parallelize(vectorCrunchTomate)

# Calcular las diferencias
rddRestaDulzura = rddDulzura.zip(rddDulzuraTomate)
restaDulzura = rddRestaDulzura.map(lambda x: (x[0] - x[1]) ** 2)

rddRestaCrunch = rddCrunch.zip(rddCrunchTomate)
restaCrunch = rddRestaCrunch.map(lambda x: (x[0] - x[1]) ** 2)

# Sumar las diferencias cuadradas y calcular la distancia euclidiana
rddDistanciaCuadrada = restaDulzura.zip(restaCrunch).map(lambda x: x[0] + x[1])
rddDistancia = rddDistanciaCuadrada.map(lambda x: math.sqrt(x))

# Asociar las distancias con los ingredientes y tipos
rddIngredienteDistancia = rddIngrediente.zip(rddDistancia)
rddTipoDistancia = rddTipo.zip(rddDistancia)

# Mostrar el resultado
resultados = rddIngredienteDistancia.collect()
for ingrediente, distancia in resultados:
    print(f"Ingrediente: {ingrediente}, Distancia: {distancia}")

# Función para determinar el tipo según la distancia más corta
def determinar_tipo():
    tipoDistancias = rddTipoDistancia.collect()
    tipoMenorDistancias = min(tipoDistancias, key=lambda x: x[1])  # Buscar el tipo con la menor distancia
    print(f"El nuevo elemento (tomate) es clasificado como: {tipoMenorDistancias[0]}")

# Llamar a la función para determinar el tipo
determinar_tipo()

sc.stop()
