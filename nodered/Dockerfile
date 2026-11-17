ARG NODERED_VERSION=4.1.16-22
FROM nodered/node-red:${NODERED_VERSION}

# FlowFuse Dashboard 2.0 (the dashboard package used in the course labs)
ARG DASHBOARD_VERSION=1.34.1
RUN npm install --no-audit --no-fund @flowfuse/node-red-dashboard@${DASHBOARD_VERSION}

# Kafka nodes used in the course labs (kafka-suite-producer / -consumer), for the MQTT -> Kafka bridge
RUN npm install --no-audit --no-fund node-red-contrib-kafka-suite@0.0.6
