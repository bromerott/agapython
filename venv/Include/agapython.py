#SENTENCIA PARA ACTIVAR VIRTUAL ENVIROMENT: cmd y luego venv\Scripts\activate
from flask import Flask, jsonify, abort
from flask import make_response
from flask import request
import time
import atexit
import asignaciones
import AlgoritmoCola
import pandas as pd
app = Flask(__name__)

#Controlador de Asignaciones Puertas-Vuelos
contAsignaciones = asignaciones.ControladorAsignaciones()
#Controlador de Algoritmo de Cola
contAlgoritmoCola = AlgoritmoCola.AlgoritmoCola()

#Metodo de retorno de las Asignaciones
@app.route('/agapython/listarAsignaciones', methods=['GET'])
def get_Asignaciones():
    return jsonify({'asignaciones': contAsignaciones.listarAsignaciones()})

#Metodo para actualizar el estado de una puerta de acuerdo a un JSON enviado
@app.route('/agapython/actualizarPuertas', methods=['GET'])
def update_Puerta():
    try:
        print("Actualizacion de Puertas!")
        contAsignaciones.cargarPuertas()
    except:
        return jsonify({'Error':'Error en la carga de base de datos'})

    return jsonify({'Puertas':'Actualizadas'})

#Metodo para listar puertas de uso interno, no agrega valor al servidor Spring
@app.route('/agapython/listarPuertas', methods=['GET'])
def get_Puertas():
    return jsonify({'Puertas': contAsignaciones.listarPuertas()})

#Metodo que recibe un dict de nuevos vuelos en forma de un JSON
#Incluye posterior algoritmo de cola para priorizacion, manejado por el algoritmo de cola
@app.route('/agapython/encolarVuelo', methods=['POST'])
def create_Vuelo():
    if not request.json:
        abort(400)
    vuelo = contAlgoritmoCola.encolarVuelo(request.json['idVuelo'],request.json['TiempoLlegada'], request.json['NPersonas'],request.json['NPrioridad'])
    return jsonify({'Vuelo': vuelo}), 201

#Metodo para matar vuelos que ya no descargan pasajeros
@app.route('/agapython/actualizarVuelos', methods=['GET'])
def update_Vuelos():
    contAlgoritmoCola.actualizarVuelos()
    contAlgoritmoCola.matarVuelos(contAsignaciones.listarAsignaciones(),contAsignaciones.listarPuertas())
    return jsonify({'Vuelos': 'Actualizados'}), 201

#Metodo para listar vuelos encolados al servidor (uso interno)
@app.route('/agapython/listarVuelos', methods=['GET'])
def get_Vuelos():
    return jsonify({'VuelosEncolados':contAlgoritmoCola.listarVuelosEncolados()}), 201

#Metodo para asignar vuelos
@app.route('/agapython/asignarVuelos', methods=['GET'])
def assign_Vuelos():
    #Matar vuelos porsiacaso
    contAlgoritmoCola.actualizarVuelos()
    contAlgoritmoCola.matarVuelos(contAsignaciones.listarAsignaciones(),contAsignaciones.listarPuertas())
    dfVuelosEscogidos = contAlgoritmoCola.escogerVuelos()
    if (dfVuelosEscogidos.shape[0]):
        contAsignaciones.asignarVuelos(dfVuelosEscogidos)
        return jsonify({'Vuelos': 'Asignados (consulte ahora servicio listarAsignaciones)'}), 201
    else:
        return jsonify({'Vuelos': 'No Asignados (nada nuevo que asignar)'}), 201
    #Vuelos ahora asignados
    
#Metodo de mapeo de errores
@app.errorhandler(404)
def not_found(error):
    return make_response(jsonify({'error': 'Not found'}), 404)
#Creacion de tarea daemon que ejecute el algoritmo de asignacion de vuelos cada 15 segundos
if __name__ == '__main__':
    app.run(debug=True,host="0.0.0.0")
