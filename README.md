# Edge-to-Cloud monitoring of simulated vital signs

Course project for *Distributed Systems for IoT* (Master in IoT, UPM).
Authors: Nora Larrañaga (Edge), Dunia Amroune (Cloud).

> **Academic simulation.** There are no real patients or medical devices. Thresholds are illustrative.

Simulated bedside monitors on a hospital ward publish pulse and SpO2 readings over **MQTT**.
At the **Edge**, Node-RED detects out-of-range values and shows alerts on an on-call dashboard,
without depending on the cloud. In parallel, every reading is forwarded to **Kafka** (Cloud part).

## Edge part

| Service | Technology | Role |
|---|---|---|
| `mosquitto` | Eclipse Mosquitto 2.0 | MQTT broker |
| `nodered` | Node-RED 4.1 + Dashboard 2.0 | Tab 1: monitor simulator · Tab 2: alert logic · Tab 3: on-call dashboard |
| `kafka`, `kafka-ui` | Kafka (KRaft, single node) + Kafka UI | Infrastructure for the Cloud part |

### Message contract
- **Readings** — `hospital/{ward}/room{room}/{sign}` (e.g. `hospital/ward2/room204/pulse`), QoS 1:
  ```json
  {"ward":"ward2","room":"204","patient":"P001","sign":"pulse","value":72,"unit":"bpm","timestamp":"2026-11-13T20:35:15.271Z"}
  ```
  3 patients (P001–P003, rooms 204–206), one pulse and one SpO2 reading each every 10 s.
  `timestamp`: ISO 8601 UTC, set by the simulator when the reading is generated (event time).
- **Alerts** — `alerts/{ward}/room{room}`, QoS 1. Only state changes: `ALERT` (pulse < 40 or > 140 bpm, SpO2 < 90 %) and `RESOLVED`.
- **Monitor status** — `status/monitors`, retained: `online` (birth) / `offline` (Last Will).

### Run
```bash
cp .env.example .env            # PowerShell: copy .env.example .env
docker compose up -d --build
```
- Dashboard: http://localhost:1880/dashboard (use the *Crisis* buttons)
- Node-RED editor: http://localhost:1880 · Kafka UI: http://localhost:8080
- Addresses: inside Docker `mosquitto:1883`, `kafka:29092` · from Windows `localhost:1883`, `localhost:9092`

### Verify
```bash
docker exec -it iot_mosquitto mosquitto_sub -t "hospital/#" -v    # 6 readings every 10 s
docker exec -it iot_mosquitto mosquitto_sub -t "alerts/#" -v      # ALERT / RESOLVED events
docker exec -it iot_mosquitto mosquitto_sub -t "status/#" -v      # online (retained)
```

## Cloud part
MQTT → Kafka bridge, history and averages consumers: see the Cloud section (Dunia).
