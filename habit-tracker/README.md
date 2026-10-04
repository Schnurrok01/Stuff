# Habit Tracker (Expo / iPhone)

Einfache Gewohnheits-App mit Expo (SDK 57), React Native und TypeScript.

## Funktionen

- Gewohnheiten mit Name, Emoji und Farbe anlegen, bearbeiten und löschen
- Heute abhaken (großer Haken rechts) – mit haptischem Feedback
- Wochenansicht der letzten 7 Tage – vergangene Tage per Tipp nachtragen
- Aktuelle Serie 🔥, längste Serie und Gesamtzahl
- Tagesfortschritt oben
- Hell- und Dunkelmodus
- Daten bleiben lokal auf dem Gerät gespeichert (AsyncStorage)

## Auf dem iPhone starten

1. Die App **Expo Go** aus dem App Store installieren.
2. Auf dem Rechner:
   ```bash
   cd habit-tracker
   npm install
   npx expo start
   ```
3. Den QR-Code mit der iPhone-Kamera scannen – die App öffnet sich in Expo Go.
   (iPhone und Rechner müssen im selben WLAN sein, sonst `npx expo start --tunnel`.)

## Als eigene App installieren (ohne Expo Go)

Mit EAS Build (Apple-Developer-Account nötig):

```bash
npx eas-cli@latest build --platform ios
```

Die Bundle-ID steht in `app.json` (`com.schnurrok.habittracker`) und kann dort angepasst werden.

## Aufbau

- `App.tsx` – Startbildschirm mit Liste, Fortschritt und „+“-Button
- `src/components/HabitCard.tsx` – Karte einer Gewohnheit mit Wochenansicht
- `src/components/HabitEditor.tsx` – Sheet zum Anlegen/Bearbeiten
- `src/useHabits.ts` – Zustand und Speicherung
- `src/date.ts` – Datums- und Serienberechnung
