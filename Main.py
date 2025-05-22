import ssl
import json
import math
import sqlite3
from datetime import datetime
import paho.mqtt.client as mqtt

# --- MQTT Config ---
BROKER = "4bbdd786011e4b5b9c59341c1329645a.s1.eu.hivemq.cloud"
PORT = 8883
USERNAME = "Python2_Recibir"
PASSWORD = "Python2_Recibir"
TOPIC_IN = "ESP32/Datos"
TOPIC_OUT = "ESP32/DatosProcesados"

# --- Parámetros ---
FACTOR_CALIBRACION = 7.5
ENERGIA_ACUMULADA = 0.0  # Wh

# --- Base de datos ---
conn = sqlite3.connect("BASE_DE_DATOS.db")
cursor = conn.cursor()
cursor.execute('''
    CREATE TABLE IF NOT EXISTS registros (
        timestamp TEXT,
        frecuencia_pulsos REAL,
        caudal_inst REAL,
        volumen_total REAL,
        estado_flujo INTEGER,
        V_rms REAL,
        I_rms REAL,
        P REAL,
        Q REAL,
        S REAL,
        PF REAL,
        desfase REAL,
        energia REAL
    )
''')
conn.commit()

# --- Función de procesamiento ---
volumen_total = 0.0
def procesar_datos(datos):
    global volumen_total, ENERGIA_ACUMULADA

    # --- Timestamp actual ---
    timestamp = datetime.utcnow().isoformat() + "Z"

    # --- Agua ---
    pulsos = int(datos.get("pulsos", 0))
    frecuencia = pulsos / 1.0  # por segundo
    caudal = frecuencia / FACTOR_CALIBRACION  # L/min
    caudal_Ls = caudal / 60.0
    volumen_total += caudal_Ls
    flujo_activo = frecuencia > 0

    # --- Electricidad ---
    V_adc = float(datos.get("V_rms_adc", 0))
    I_adc = float(datos.get("I_rms_adc", 0))

    V_rms = V_adc * 0.1
    I_rms = I_adc * 0.1
    S = V_rms * I_rms  # VA
    PF = 0.85  # asumido constante
    P = S * PF  # W
    Q = math.sqrt(S**2 - P**2)  # VAR
    desfase = math.degrees(math.acos(PF))  # grados
    ENERGIA_ACUMULADA += P / 3600.0  # Wh (cada segundo)

    # --- JSON estructurado ---
    json_final = {
        "timestamp": timestamp,
        "DatosAgua": {
            "frecuencia_pulsos": round(frecuencia, 2),
            "caudal_inst": round(caudal, 2),
            "volumen_total": round(volumen_total, 3),
            "estado_flujo": flujo_activo
        },
        "DatosElectricidad": {
            "V_rms": round(V_rms, 2),
            "I_rms": round(I_rms, 2),
            "S": round(S, 2),
            "P": round(P, 2),
            "Q": round(Q, 2),
            "PF": round(PF, 2),
            "desfase": round(desfase, 2),
            "energia": round(ENERGIA_ACUMULADA, 4)
        }
    }

    # --- Guardar en la base de datos ---
    cursor.execute('''
        INSERT INTO registros VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        timestamp,
        frecuencia, caudal, volumen_total, int(flujo_activo),
        V_rms, I_rms, P, Q, S, PF, desfase, ENERGIA_ACUMULADA
    ))
    conn.commit()

    return json_final

# --- Callbacks MQTT ---
def on_connect(client, userdata, flags, rc):
    print("📡 Conectado al broker MQTT")
    client.subscribe(TOPIC_IN)

def on_message(client, userdata, msg):
    try:
        datos = json.loads(msg.payload.decode())
        datos_procesados = procesar_datos(datos)

        print("\n✅ Datos procesados:")
        print(json.dumps(datos_procesados, indent=2))

        # Publicar al topic final
        client.publish(TOPIC_OUT, json.dumps(datos_procesados))

    except Exception as e:
        print("[ERROR procesando mensaje]", e)

# --- MQTT Setup ---
client = mqtt.Client()
client.username_pw_set(USERNAME, PASSWORD)
client.tls_set(tls_version=ssl.PROTOCOL_TLS)
client.on_connect = on_connect
client.on_message = on_message

client.connect(BROKER, PORT, 60)
client.loop_forever()

