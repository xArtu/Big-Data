from pyspark import SparkContext

sc = SparkContext("local", "tercerContext")

# ... (tus datos)

rddDatos = rddIngrediente.zip(rddDulzura).zip(rddCrunch).map(lambda x: (x[0][0], x[0][1], x[1]))

def distanciaEuclidiana(punto1, punto2):
    # ... (definición de la distancia)

K = 3
nuevoPunto = ("tomate", 6, 4)

distancias = rddDatos.map(lambda x: (x, distanciaEuclidiana(x, nuevoPunto)))
vecinosCercanos = distancias.sortBy(lambda x: x[1]).take(K)

for vecino in vecinosCercanos:
    print(f"Vecino cercano: {vecino[0][0]}")

sc.stop()