import SimulatedAnnealing
import mysql.connector
import pandas as pd
#Controlador de Puertas y Asignaciones
class ControladorAsignaciones():
    asignaciones = list()
    dfPuertas = pd.DataFrame()
    def __init__(self):
        #Cargar lista vacia de asignaciones
        for i in range(1,40):
            asignacion = {
                'idPuerta':i,
                'idVueloAsignado':''
            }
            self.asignaciones.append(asignacion)
        #Cargar puertas desde la BD
        self.cargarPuertas()

    def matarAsignaciones(self,codigosMuertos):
        for asignacion in self.asignaciones:
            if asignacion['idVueloAsignado'] in codigosMuertos:
                asignacion['idVueloAsignado']=""

    def listarAsignaciones(self):
        return self.asignaciones

    def listarPuertas(self):
        return self.dfPuertas.to_dict('records')

    def cargarPuertas(self):
        #Cargar las puertas (nPuerta,Tipo,FlujoPersonas, Estado) desde la BD
        mydb = mysql.connector.connect(
            host="127.0.0.1",
            user="root",
            passwd="",
            port="3306",
            database="AGAPORT"
        )
        mydb.connect()
        self.dfPuertas = pd.read_sql_query("SELECT id_puerta,tipo,distanciaasalida,flujo_personas,estado,borrado FROM puertas; ",mydb)
        self.dfPuertas = self.dfPuertas.rename(columns={"id_puerta":"idPuerta","flujo_personas":"FlujoPersonas","distanciaasalida":"DistanciaASalida","tipo":"Tipo","estado":"Estado"})
        mydb.close()
        return 0
    def asignarVuelos(self,dfVuelosEscogidos):
        #Sacar relacion codigoPuertas -> nuevo dfPuertas
        dfPuertasDisp = self.dfPuertas[self.dfPuertas['Estado']==1]
        #Quitar puertas asignadas
        for asignacion in self.asignaciones:
            if (asignacion['idVueloAsignado']):
                index = dfPuertasDisp[ dfPuertasDisp['idPuerta'] == asignacion['idPuerta'] ].index
                dfPuertasDisp = dfPuertasDisp.drop(index)
        print("dfPuertasDisp")
        print(dfPuertasDisp)
        resultado = SimulatedAnnealing.SimulatedAnnealing(dfPuertasDisp,dfVuelosEscogidos)
        print("Resultado de SA")
        print(resultado)
        #A partir del resultado del algoritmo, actualizar la estructura dict asignaciones
        #Resultado es un dataframe donde cada columna es un arreglo de 39 columnas con 0s y 1s
        for index, row in resultado.iterrows():
            #index corresponde al i-esimo elemento de dfVuelos escogidos
            codVueloAsignado = dfVuelosEscogidos.iloc[index]['idVuelo']
            print("codVueloAsignado:",codVueloAsignado)
            #Sacar ocurrencia de 1 en row
            nPuertaAsignada=-1
            i=0
            for e in row:
                if (e==1):
                    nPuertaAsignada = i
                i=i+1
            print("nPuertaAsignada:",nPuertaAsignada)
            #Relacionar ocurrencia de 1 (sistema de filas de SA) con idPuerta
            codPuertaAsignada = dfPuertasDisp.iloc[nPuertaAsignada]['idPuerta']
            print("codPuertaAsignada:",codPuertaAsignada)
            #Actualizar BD interna de asignaciones
            for asignacion in self.asignaciones:
                if (asignacion['idPuerta'] == codPuertaAsignada):
                    asignacion['idVueloAsignado'] = codVueloAsignado
        return resultado
