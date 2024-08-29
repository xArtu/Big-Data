import csv
import math

def calcularDistancia(dulce1, crujiente1, dulce2, crujiente2):
    return math.sqrt((dulce1 - dulce2)**2 + (crujiente1 - crujiente2)**2)

def clasificarIngrediente(dulce, crujiente, ingredientes):
    min_distancia = float('inf')
    clasificacion = None

    print("Comparando con los ingredientes existentes:\n")

    for ingrediente in ingredientes:
        distancia = calcularDistancia(dulce, crujiente, ingrediente['dulce'], ingrediente['crujiente'])
        print(f"{ingrediente['ingrediente']}: distancia = {distancia:.2f}")
        if distancia < min_distancia:
            min_distancia = distancia
            clasificacion = ingrediente['sabor']

    return clasificacion

def leerCSV(nombreArchivo):
    ingredientes = []
    with open(nombreArchivo, mode='r', newline='') as file:
        reader = csv.DictReader(file)
        for row in reader:
            ingredientes.append({
                'ingrediente': row['ingrediente'],
                'dulce': int(row['dulce']),
                'crujiente': int(row['crujiente']),
                'sabor': row['sabor']
            })
    return ingredientes

def escribirCSV(nombreArchivo, ingrediente, dulce, crujiente, sabor):
    with open(nombreArchivo, mode='a', newline='\n') as file:
        writer = csv.writer(file)
        writer.writerow([ingrediente, dulce, crujiente, sabor])


def main():
    nombreArchivo = 'alimentos.csv'
    ingredientes = leerCSV(nombreArchivo)

    nombreIngrediente = input("Ingrese el nombre del nuevo ingrediente: ")
    dulce = int(input("Ingrese el nivel de dulzura (1-10): "))
    crujiente = int(input("Ingrese el nivel de crujientez (1-10): "))

    clasificacion = clasificarIngrediente(dulce, crujiente, ingredientes)
    print(f"El ingrediente {nombreIngrediente} ha sido clasificado como: {clasificacion}")

    escribirCSV(nombreArchivo, nombreIngrediente, dulce, crujiente, clasificacion)
    print(f"El ingrediente {nombreIngrediente} ha sido agregado al archivo CSV.")

if __name__ == "__main__":
    main()
