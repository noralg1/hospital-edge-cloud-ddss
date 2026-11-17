# Hospital vital signs monitoring (Edge to Cloud)

Distributed Systems for IoT

Three simulated monitors send pulse and SpO2 readings over MQTT. Node-RED checks the values and shows alerts on a dashboard, and the readings also go to Kafka for the Cloud part. It is a simulation, there are no real patients.

## Services

- `mosquitto`: MQTT broker
- `nodered`: monitor simulator, alert logic and dashboard (one tab each)
- `kafka` and `kafka-ui`: Kafka in KRaft mode, used by the Cloud part

## MQTT topics

Readings are published every 10 s to `hospital/{ward}/room{room}/{sign}` with QoS 1, for example:

```json
{"ward":"ward2","room":"204","patient":"P001","sign":"pulse","value":72,"unit":"bpm","timestamp":"2026-11-13T20:35:15.271Z"}
```

The timestamp is set by the simulator when the reading is generated (UTC).

Alerts go to `alerts/{ward}/room{room}` with QoS 1. Only changes are sent: ALERT when pulse is below 40 or above 140, or SpO2 below 90, and RESOLVED when it goes back to normal.

`status/monitors` is retained and says `online`, or `offline` when Node-RED disconnects (Last Will).

## How to run

```
copy .env.example .env
docker compose up -d --build
```

Dashboard: http://localhost:1880/dashboard  
Node-RED: http://localhost:1880  
Kafka UI: http://localhost:8080

To see the messages:

```
docker exec -it iot_mosquitto mosquitto_sub -t "hospital/#" -v
docker exec -it iot_mosquitto mosquitto_sub -t "alerts/#" -v
```
