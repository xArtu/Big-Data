import math
from pyspark import SparkContext

#crear spark context
sc = SparkContext("local", "SaboresContext")

ingrediente=["uva","haba","Nuez", "Naranja"]
dulzura=[8,3,3,7]
crunch=[5,7,6,3]
tipo=["fruta","vegetal","proteina", "fruta"]
dulzuratomate =[6,6,6,6]
crunchtomate =[4,4,4,4]

rdd1=sc.parallelize(ingrediente)
rdd2=sc.parallelize(dulzura)
rdd3=sc.parallelize(crunch)
rdd4=sc.parallelize(tipo)
rdd5=sc.parallelize(dulzuratomate)
rdd6=sc.parallelize(crunchtomate)


rddrestax=rdd2.zip(rdd5)
restadulzura=rddrestax.map(lambda x: x[0] - x[1]) #mandas y recibes un vector con la misma longitud (map)
rddCuadradox=restadulzura.map(lambda x: x * x) #Elevado al cuadrado

rddrestay=rdd3.zip(rdd6)
restacrunch=rddrestay.map(lambda y: y[0] - y[1])
rddCuadradoy=restacrunch.map(lambda y: y * y)

# Sumar los cuadrados
rddSuma = rddCuadradox.zip(rddCuadradoy).map(lambda x: x[0] + x[1])

# Calcular la raíz cuadrada (distancia euclidiana)
rddDistancia = rddSuma.map(lambda x: math.sqrt(x))

# Unir las distancias con los tipos de ingredientes
rddDistanciasConTipo = rddDistancia.zip(rdd4)

# Mostrar las distancias y el tipo de ingrediente
distanciasConTipo = rddDistanciasConTipo.collect()

# Imprimir el resultado
for distancia, tipo in distanciasConTipo:
    print(f"Distancia al {tipo}: {distancia}")

# Encontrar la distancia mínima
distanciaMinima, tipoIngrediente = min(distanciasConTipo, key=lambda x: x[0])

# Imprimir el tipo más cercano
print(f"\nEl tomate se clasifica como: {tipoIngrediente}")


sc.stop()