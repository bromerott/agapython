import SimulatedAnnealing
import mysql.connector
import pandas as pd
#Controlador de Puertas y Asignaciones
class ControladorAsignaciones():
    asignaciones = []
    dfPuertas = pd.DataFrame()
    def __init__(self):
        #Cargar lista vacia de asignaciones
        for i in range(1,40):
            asignacion = {
                'idPuerta':i,
                'idVueloAsignado':''
            }
            self.asignaciones = self.asignaciones.append(asignacion)
        #Cargar puertas desde la BD
        self.cargarPuertas()

    def listarAsignaciones(self):
        return self.asignaciones

    def listarPuertas(self):
        return self.dfPuertas.to_dict('records')

    def cargarPuertas(self):
        #Cargar las puertas (nPuerta,Tipo,FlujoPersonas, Estado) desde la BD
        mydb = mysql.connector.connect(
            host="200.16.7.178",
            user="bromero",
            passwd="bromero",
            port="3306",
            database="AGAPORT"
        )
        mydb.connect()
        print(mydb)
        self.dfPuertas = pd.read_sql_query("SELECT id_puerta,tipo,distanciaasalida,flujo_personas,estado,borrado FROM puertas; ",mydb)
        self.dfPuertas = self.dfPuertas.rename(columns={"id_puerta":"idPuerta","flujo_personas":"FlujoPersonas","distanciaasalida":"DistanciaASalida","tipo":"Tipo","estado":"Estado"})
        #print(self.dfPuertas)
        mydb.close()
        return 0
    def asignarVuelos(self,dfVuelosEscogidos):
        resultado = SimulatedAnnealing.SimulatedAnnealing(self.dfPuertas,dfVuelosEscogidos)
        #A partir del resultado del algoritmo, actualizar la estructura dict asignaciones
        print(resultado)
        return resultado
