import numpy as np
import pandas as pd
from pandas import DataFrame, Series
import random
from numpy.random import randn
from math import exp
from datetime import datetime
import copy

#Crear modelo
def CrearModelo(Gates,Llegadas):
    #Conseguir 38 datos
    #Cargar Datos
    #Por ahora, trabajar solo con las primeras 30 llegadas
    Llegadas_shape = Llegadas.shape
    Llegadas_ordenadas = Llegadas.sort_values(['TiempoLlegada'], ascending=[True])
    #Output
    return {'Llegadas':Llegadas,'Llegadas_shape':Llegadas_shape,'Llegadas_ordenadas':Llegadas_ordenadas, 'Gates':Gates}

#Funcion crear solucion inicial
def SolucionIni(modelo):
    J = modelo['Gates'].shape[0]
    I = modelo['Llegadas_shape'][0]
    #Objetivo: Llegar a una solucion Xij que asigne el vuelo i a la puerta j
    X = DataFrame(0, index= range(I), columns = np.arange(J))
    
    #Solucion Inicial: Asignacion diagonal
    for i in range(I):
        X[i][i]=1
    #Shuffling
    X = X.sample(frac=1).reset_index(drop=True)
    return{'X':X}

#Calcular Funcion Objetivo de solucion inicial
def calcularObjetivo(modelo,X):
    filas = X.shape[0]
    columnas = X.shape[1]
    costo = 0
    for i in range(1,filas):
        for j in range(1,columnas):
            if (X[j][i] == 1):
                nPasajeros= float( modelo['Llegadas'].iloc[i]['NPersonas'])
                caudal= float( modelo['Gates'].iloc[j]['FlujoPersonas'])
                costo = costo + nPasajeros/ caudal
    return costo

#Funcion que toma una solucion y la cambia a un "vecino"
def crearVecino(modelo,q):
    xx = copy.deepcopy(q['X'])
    n = len(q['X'])
    if (n>1):
        i = random.sample(range(n), 2)
        i1 = i[0]
        i2 = i[1]
        var = copy.deepcopy(xx.iloc[i2].values)
        xx.iloc[i2] = xx.iloc[i1]
        xx.iloc[i1] = var
    return {'xx':xx}

#Funcion main
## Parametros de S.A ##
def SimulatedAnnealing(dfPuertas,dfLlegadas):
    MaxIt = 1        # Iteraciones Maximas
    MaxIt2 = 30      # Iteraciones Internas Maximas
    T0 = 10          # Temperatura Inicial
    alpha = 0.8     # Grado de decay

    modelo = CrearModelo(dfPuertas,dfLlegadas)
    solucion = SolucionIni(modelo)
    foIni = calcularObjetivo(modelo,solucion['X'])
    
    #Loop de SA
    mejorSolucion = solucion
    mejorFo = foIni
    T = T0

    for it in range(MaxIt):
        solucion = SolucionIni(modelo)
        foIni = calcularObjetivo(modelo,solucion['X'])
        for it2 in range(MaxIt2):
            vecino = crearVecino(modelo,solucion)
            nuevoFo = calcularObjetivo(modelo,vecino['xx'])
            #Si la F.O ha subido, es la nueva mejor!
            if (nuevoFo > mejorFo):
                mejorSolucion = solucion
                mejorFo = nuevoFo
                #Moverse a esa solucion
                solucion['X'] = vecino['xx']
            #En el caso que no haya subido, arriesgarse o no en base a Temperatura
            else:
                delta = nuevoFo - calcularObjetivo(modelo,solucion['X'])
                p = exp(-delta / T)
                rand = random.uniform(0, 1)
                if rand <= p:
                    #Si la probabilidad se da, moverse a esa solucion
                    solucion['X'] = vecino['xx']

            #Reducir Temperatura
            T = T0*alpha
            
    #Transformar solucion['X'] (un arreglo de 0s y 1s a una lista de dicts de Asignaciones)
    return (mejorSolucion['X'])