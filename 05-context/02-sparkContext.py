from pyspark import SparkContext

sc = SparkContext("local", "segundoContext")

vector = [1,2,3,4,5]
rdd = sc.parallelize(vector)
indice = rdd.zipWithIndex()
rddParticular = indice.filter(lambda x:x[0]==3).collect()[0][1]

suma = rdd.reduce(lambda x,y: x+y)

print(rddParticular)
print(indice.collect())
print(suma)

sc.stop()