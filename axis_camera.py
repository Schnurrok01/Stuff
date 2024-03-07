import paho.mqtt.client as mqtt
import json
import pandas as pd
import time
import matplotlib.pyplot as plt

# Zusätzliche Importe für das Schreiben in Excel
from datetime import datetime

# Initialisiere den DataFrame
df = pd.DataFrame(columns=['Startzeit', 'Endzeit', 'Summe Active'])

# MQTT-Konfiguration
TOPIC = "Lab/event/tns:axis/CameraApplicationPlatform/ObjectAnalytics/Device1Scenario3"
BROKER_ADDRESS = "test.mosquitto.org"
PORT = 1883
QOS = 0

# Initialisiere die Variablen
startzeit = time.time()
summe_active = 0
intervall = 15  # 10 Sekunden

# Funktion zum Verarbeiten und Speichern der Daten
def verarbeite_daten():
    global startzeit, summe_active, df
    endzeit = startzeit + intervall
    new_record = pd.DataFrame({'Startzeit': [startzeit], 'Endzeit': [endzeit], 'Summe Active': [summe_active]})
    df = pd.concat([df, new_record], ignore_index=True)
    startzeit = time.time()
    summe_active = 0


# Callback-Funktion für eingehende MQTT-Nachrichten
def on_message(client, userdata, message):
    global startzeit, summe_active

    msg_ = str(message.payload.decode("utf-8"))
    json_msg = json.loads(msg_)

    timestamp = time.time()
    if timestamp < startzeit + intervall:
        if 'message' in json_msg and 'data' in json_msg['message'] and 'active' in json_msg['message']['data']:
            summe_active += int(json_msg['message']['data']['active'])
    else:
        verarbeite_daten()

# Callback-Funktion für die Verbindungsherstellung
def on_connect(client, userdata, flags, rc):
    print("Connected to MQTT Broker: " + BROKER_ADDRESS)
    client.subscribe(TOPIC)

# Hauptprogramm
if __name__ == "__main__":
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(BROKER_ADDRESS, PORT)
    client.loop_start()

    try:
        while True:
            time.sleep(1)
            if time.time() > startzeit + intervall:
                verarbeite_daten()
    except KeyboardInterrupt:
        client.loop_stop()
        # Speichere den DataFrame in eine Excel-Datei, sobald das Programm beendet wird
        
        # Plotte die Daten, sobald sie verarbeitet sind
        plt.figure(figsize=(10, 6))
        plt.plot(df['Endzeit'], df['Summe Active'], marker='o', linestyle='-')
        plt.title('Summe Active über die Zeit')
        plt.xlabel('Zeit (s)')
        plt.ylabel('Summe Active')
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.show()
        
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        excel_filename = f"MQTT_Daten_{timestamp}.xlsx"
        df.to_excel(excel_filename, index=False)
        print(f"DataFrame wurde in '{excel_filename}' gespeichert.")
