ARG NODERED_BASE_IMAGE=nodered/node-red:latest
FROM ${NODERED_BASE_IMAGE}

# Necesitamos root para instalar paquetes npm en el árbol de la imagen
USER root
WORKDIR /usr/src/node-red

# node-red-dashboard: mencionado explícitamente en Lesson 1
#   (https://flows.nodered.org/node/@flowfuse/node-red-dashboard)
# node-red-contrib-kafka-manager: nodos de Kafka para los Labs 3/4 de GA-506
RUN npm install --no-update-notifier --no-fund \
      @flowfuse/node-red-dashboard \
      node-red-contrib-kafka-manager \
  && npm cache clean --force

USER node-red
