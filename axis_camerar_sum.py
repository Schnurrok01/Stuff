import paho.mqtt.client as mqtt
import json
import pandas as pd
import time
from datetime import datetime
import matplotlib.pyplot as plt

# Initialisiere den DataFrame mit metrischem Zeitformat
df = pd.DataFrame(columns=['Startzeit', 'Endzeit', 'Mittlere Anzahl Personen'])

# MQTT-Konfiguration
TOPIC = "Lab/event/tns:axis/CameraApplicationPlatform/ObjectAnalytics/Device1Scenario3"
BROKER_ADDRESS = "test.mosquitto.org"
PORT = 1883
QOS = 0

# Initialisiere die Variablen
startzeit = time.time()
gesamtanzahl_personen = 0
messungen = 0
intervall = 60  # 60 Sekunden

# Funktion zum Verarbeiten und Speichern der Daten
def verarbeite_daten():
    global startzeit, gesamtanzahl_personen, messungen, df
    endzeit = startzeit + intervall
    mittlere_anzahl_personen = gesamtanzahl_personen / messungen if messungen > 0 else 0
    # Umwandlung von UNIX-Zeitstempeln in metrisches Format
    startzeit_str = datetime.fromtimestamp(startzeit).strftime('%Y-%m-%d %H:%M:%S')
    endzeit_str = datetime.fromtimestamp(endzeit).strftime('%Y-%m-%d %H:%M:%S')
    new_record = pd.DataFrame({'Startzeit': [startzeit_str], 'Endzeit': [endzeit_str], 'Mittlere Anzahl Personen': [mittlere_anzahl_personen]})
    df = pd.concat([df, new_record], ignore_index=True)
    startzeit = time.time()
    gesamtanzahl_personen = 0
    messungen = 0

# Callback-Funktion für eingehende MQTT-Nachrichten
def on_message(client, userdata, message):
    global startzeit, gesamtanzahl_personen, messungen

    msg_ = str(message.payload.decode("utf-8"))
    json_msg = json.loads(msg_)

    timestamp = time.time()
    if timestamp < startzeit + intervall:
        if 'message' in json_msg and 'data' in json_msg['message'] and 'human' in json_msg['message']['data']:
            gesamtanzahl_personen += int(json_msg['message']['data']['human'])
            messungen += 1
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
        # Plot und Speicherung der Daten
        plt.figure(figsize=(10, 6))
        plt.plot(pd.to_datetime(df['Endzeit']), df['Mittlere Anzahl Personen'], marker='o', linestyle='-')
        plt.title('Mittlere Anzahl Personen über die Zeit')
        plt.xlabel('Zeit')
        plt.ylabel('Mittlere Anzahl Personen')
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.show()
        
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        excel_filename = f"MQTT_Daten_{timestamp}.xlsx"
        df.to_excel(excel_filename, index=False)
        print(f"DataFrame wurde in '{excel_filename}' gespeichert.")
