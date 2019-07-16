import pandas as pd
from datetime import datetime, timedelta

class AlgoritmoCola():
    #Tamaño maximo de input al algoritmo
    bufferSize = 38
    #Vuelos en cola almacenados en un Dataframe
    dfVuelosEncolados = pd.DataFrame()
    #Definicion de columnas del dataframe
    def __init__(self):
        self.dfVuelosEncolados = pd.DataFrame(columns=['idVuelo','TiempoLlegada','NPersonas','NPrioridad','Asignado','Estado'])
    def listarVuelosEncolados(self):
        return self.dfVuelosEncolados.to_json(orient='records')
    #Listar filas cuyo estado sea =3
    def listarVuelosMuertos(self):
        return self.dfVuelosEncolados.loc[self.dfVuelosEncolados["Estado"]==3].to_json(orient='records')
    #Encolacion del vuelo al algoritmo
    def encolarVuelo(self,idVuelo,TiempoLlegada,NPersonas,NPrioridad):
        #Parsing de string TiempoLlegada a datetime
        dtLlegada = datetime.strptime(TiempoLlegada,'%Y-%m-%d %H:%M:%S')
        self.dfVuelosEncolados = self.dfVuelosEncolados.append({"idVuelo":idVuelo,"TiempoLlegada":dtLlegada,"NPersonas":NPersonas,"NPrioridad":NPrioridad,"Asignado":0,"Estado":0}, ignore_index=True)
        return self.dfVuelosEncolados[self.dfVuelosEncolados['idVuelo']==idVuelo].to_json(orient ='records')
    
    def actualizarVuelos(self):
        #Chequeo de vuelos aterrizados
        for row in self.dfVuelosEncolados:
            tiempoLlegada = row['TiempoLlegada'][0]
            if (tiempoLlegada>datetime.datetime.now()):
                self.dfVuelosEncolados["Estado"][row["idVuelo"]]=2
    
    def matarVuelos(self,asignaciones,puertas):
        #Chequeo de vuelos que ya descargaron pasajeros y mueren para el sistema
        for asignacion in asignaciones:
            if (asignacion['idVueloAsignado']):
                #Busqueda en dict puertas
                Flujo=0
                for puerta in puertas:
                    if (puerta['id_puerta'] == asignacion['id_puerta']):
                        Flujo = puerta['FlujoPersonas']

                idVuelo = asignacion['idVueloAsignado']
                #Busqueda en dfVuelosEncolados
                row = self.dfVuelosEncolados[self.dfVuelosEncolados['idVuelo'] == idVuelo]
                tiempoCalculado = row['TiempoLlegada'][0]+(Flujo*row['NPersonas'][0])
                #Chequeo de muerte
                if (datetime.datetime.now() > tiempoCalculado):
                    #Matar vuelo, desencolandolo para siempre
                    self.dfVuelosEncolados["Estado"][row["idVuelo"]]=3
        return 0

    def escogerVuelos(self):
        #Objetivo: A partir del dfVuelosEncolados, escoger cuales seran asignados por el algoritmo, y cuales esperan
        dfNoAsignados = self.dfVuelosEncolados[self.dfVuelosEncolados['Asignado']==0]
        #Calcular cuanto espacio puedo asignar a nuevos vuelos (bufferSize maximo - asignados (y no droppeados))
        espacioSlack = self.bufferSize - dfNoAsignados.shape[0]
        #Orden de eleccion: Ordenar por hora de llegada, luego por NPrioridad, y luego por NPersonas
        #Vuelo que llega 11am consigue sitio antes que el vuelo que llega 12am
        #Para dos vuelos que llegan 12am, consigue sitio el de la aerolinea de mayor Prioridad
        #Para dos vuelos que llegan a la misma 'hora' de la misma aerolinea, primero consigue sitio el mas grande
        #Una vez 38 vuelos hayan sido escogidos, ponerlos como 'asignados'
        dfVuelosEscogidos = pd.DataFrame()
        if (espacioSlack>0 and dfNoAsignados.shape[0]>0):
            #Definicion de columna 'horaLlegada'
            dateLlegada = dfNoAsignados['TiempoLlegada'][0]
            dfNoAsignados['horaLlegada'] = dateLlegada.hour

            #dataframe ordenadito
            dfOrdenado = dfNoAsignados.sort_values(['horaLlegada', 'NPrioridad','NPersonas'], ascending=[False, False,False])
            dfVuelosEscogidos = dfOrdenado[1:min(espacioSlack,dfOrdenado.shape[0])]
            codigosEscodigos = dfVuelosEscogidos['idVuelo']

            #Con los codigos de los vuelos escogidos, actualizar el atributo Asignado
            for codigo in codigosEscodigos:
                self.dfVuelosEncolados[self.dfVuelosEncolados['idVuelo']==codigo]['Asignado'] =1
                self.dfVuelosEncolados[self.dfVuelosEncolados['idVuelo']==codigo]['Estado'] = 1

        return dfVuelosEscogidos