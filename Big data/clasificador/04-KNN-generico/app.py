import csv
import math


def leerCSV(archivoCSV):
    vectores = []
    idsDiagnosis = []
    with open(archivoCSV, newline='', encoding='utf-8') as archivo:
        lector_csv = csv.reader(archivo)
        next(lector_csv)
        for linea in lector_csv:
            idVal = linea[0]
            diagnosis = linea[1]
            vector = [float(i) for i in linea[2:]]  # Omitir 'id' y 'diagnosis' y convertir a float
            vectores.append(vector)
            idsDiagnosis.append((idVal, diagnosis))  # Guardar id y diagnosis
    return vectores, idsDiagnosis

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

archivoCSV = 'data.csv'
vectores, idsDiagnosis = leerCSV(archivoCSV)

# Nuevo vector
nuevoVector = [13.54,14.36,87.46,566.3,0.09779,0.08129,0.06664,0.04781,0.1885,0.05766,0.2699,0.7886,2.058,23.56,0.008462,0.0146,0.02387,0.01315,0.0198,0.0023,15.11,19.26,99.7,711.2,0.144,0.1773,0.239,0.1288,0.2977,0.07259]


n = 100  #Rango de muestras a imprimir / comparar
vectores_similares = vectoresSimilares(vectores, idsDiagnosis, nuevoVector, n)

#Mostra el n número más similar de vectores
for idVal, diagnosis, distancia in vectores_similares:
    print(f"ID: {idVal}, Diagnóstico: {diagnosis}, Distancia: {distancia:.4f}")


#Spark session
#Spark context