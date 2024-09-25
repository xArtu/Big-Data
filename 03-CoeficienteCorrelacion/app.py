import csv
import math

def leerCSV(archivo):
    valoresX = []
    
    valoresY = []

    with open(archivo, newline='\n') as csvfile:
        reader = csv.reader(csvfile)
        for fila in reader:
            valoresX.append(float(fila[0])) 
            # Agregar cada valor Y
            if not valoresY:
                valoresY = [[] for _ in range(len(fila) - 1)]
            for i in range(1, len(fila)):
                valoresY[i - 1].append(float(fila[i]))
    return valoresX, valoresY


def coeficientePearson(x, y):
    n = len(x)
    sumX= sum(x)
    sumY= sum(y)
    sumXY = sum(x[i] * y[i] for i in range(n))
    sumX2 = sum(xi ** 2 for xi in x)
    sumY2 = sum(yi ** 2 for yi in y)

    numerador = (sumX* sumY) - (n * sumXY)
    denominador = math.sqrt((n * sumX2 - sumX** 2) * (n * sumY2 - sumY** 2))

    if denominador == 0:
        return 0  #No hay correlacion
    return numerador / denominador

def clasificarCorrelacion(correlacion):
    if 0.96 <= correlacion <= 1:
        return "Perfecta"
    elif 0.85 <= correlacion < 0.96:
        return "Fuerte"
    elif 0.7 <= correlacion < 0.85:
        return "Significativa"
    elif 0.5 <= correlacion < 0.7:
        return "Moderada"
    elif 0.2 <= correlacion < 0.49:
        return "Débil"
    elif 0.1 <= correlacion < 0.2:
        return "Muy débil"
    else:
        return "Nula"

def calcularCorrelacion(archivo):
    x, columnasY = leerCSV(archivo)

    for i, y in enumerate(columnasY):
        correlacion = coeficientePearson(x, y)
        clasificacion = clasificarCorrelacion(abs(correlacion))
        print(f"Correlación entre X y Y{i+1}: {abs(correlacion):.4f} ({clasificacion})")

archivo = '/home/arturo/Big Data/03-CoeficienteCorrelacion/vectores.csv'
calcularCorrelacion(archivo)