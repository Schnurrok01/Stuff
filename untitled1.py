import paho.mqtt.client as mqtt
import json
import pandas as pd
import time
import matplotlib.pyplot as plt
from datetime import datetime, timedelta  # Importiere timedelta

# Initialisiere den DataFrame
df = pd.DataFrame(columns=['Startzeit', 'Endzeit', 'Summe Active'])

# MQTT-Konfiguration
TOPIC = "axis/B8A44F473ED7/event/CameraApplicationPlatform/ObjectAnalytics/Device1Scenario1"
BROKER_ADDRESS = "test.mosquitto.org"
PORT = 1883
QOS = 0

# Initialisiere die Variablen
startzeit = datetime.now()
summe_active = 0
intervall = timedelta(seconds=60)  # Definiere das Intervall als timedelta

# Funktion zum Verarbeiten und Speichern der Daten
def verarbeite_daten():
    global startzeit, summe_active, df
    endzeit = datetime.now()
    new_record = pd.DataFrame({'Startzeit': [startzeit.strftime("%Y-%m-%d %H:%M:%S")], 'Endzeit': [endzeit.strftime("%Y-%m-%d %H:%M:%S")], 'Summe Active': [summe_active]})
    df = pd.concat([df, new_record], ignore_index=True)
    startzeit = datetime.now()  # Aktualisiere Startzeit zur aktuellen Zeit
    summe_active = 0

# Callback-Funktion für eingehende MQTT-Nachrichten
def on_message(client, userdata, message):
    global startzeit, summe_active

    msg_ = str(message.payload.decode("utf-8"))
    json_msg = json.loads(msg_)

    if datetime.now() < startzeit + intervall:  # Verwende timedelta für die Zeitberechnung
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
            if datetime.now() > startzeit + intervall:  # Verwende timedelta für die Zeitberechnung
                verarbeite_daten()
    except KeyboardInterrupt:
        client.loop_stop()
        
        # Plotte die Daten, sobald sie verarbeitet sind
        plt.figure(figsize=(10, 6))
        plt.plot(df['Endzeit'], df['Summe Active'], marker='o', linestyle='-')
        plt.title('Summe Active über 30 Sekunden')
        plt.xlabel('Uhrzeit')
        plt.ylabel('Summe Active')
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.show()
        
        # Speichere den DataFrame in eine Excel-Datei
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        excel_filename = f"MQTT_Daten_{timestamp}.xlsx"
        df.to_excel(excel_filename, index=False)
        print(f"DataFrame wurde in '{excel_filename}' gespeichert.")
