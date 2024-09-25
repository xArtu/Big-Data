from pyspark import SparkContext
sc = SparkContext("local[*]", "Ejercicio") # [n] -> nucleos para trabajar

rdd = sc.parallelize(range(1, 100000000), numSlices=4) # -> doble del numero de nucleos de la PC 
suma = rdd.reduce(lambda x,y : x+y)
print(suma)
sc.stop()