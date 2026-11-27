#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MOSQUITTO_DIR="$ROOT/mosquitto"
CERT_DIR="$MOSQUITTO_DIR/certs"
PASSWORD_FILE="$MOSQUITTO_DIR/.mqtt-password"
PASSWD_FILE="$MOSQUITTO_DIR/passwd"
MQTT_HOSTNAME="${MQTT_HOSTNAME:-mqtt.jokocloud.my.id}"
MQTT_USERNAME="${MQTT_USERNAME:-user}"
IMAGE="${MOSQUITTO_IMAGE:-eclipse-mosquitto:2}"

mkdir -p "$CERT_DIR"

if [[ ! -f "$CERT_DIR/server.crt" || ! -f "$CERT_DIR/server.key" || "${FORCE:-0}" == "1" ]]; then
  TMP_CERT="$(mktemp -d)"
  openssl req -x509 -newkey rsa:2048 -sha256 -days 3650 -nodes \
    -keyout "$TMP_CERT/server.key" \
    -out "$TMP_CERT/server.crt" \
    -subj "/CN=$MQTT_HOSTNAME" \
    -addext "subjectAltName=DNS:$MQTT_HOSTNAME,DNS:localhost,IP:127.0.0.1" \
    >/dev/null 2>&1
  chmod 600 "$TMP_CERT/server.key"
  chmod 644 "$TMP_CERT/server.crt"
  mv -f "$TMP_CERT/server.key" "$CERT_DIR/server.key"
  mv -f "$TMP_CERT/server.crt" "$CERT_DIR/server.crt"
  rmdir "$TMP_CERT"
  docker run --rm --entrypoint chown -v "$CERT_DIR:/certs" "$IMAGE" \
    1883:1883 /certs/server.crt /certs/server.key
fi

if [[ ! -s "$PASSWORD_FILE" ]]; then
  openssl rand -hex 16 | cut -c1-24 > "$PASSWORD_FILE"
  chmod 600 "$PASSWORD_FILE"
fi

if [[ ! -f "$PASSWD_FILE" || "${FORCE:-0}" == "1" ]]; then
  TMP_PASS="$(mktemp -d "$ROOT/.mqtt-setup.XXXXXX")"
  docker run --rm --user "$(id -u):$(id -g)" --entrypoint sh \
    -e MQTT_USERNAME="$MQTT_USERNAME" \
    -e MQTT_PASSWORD="$(cat "$PASSWORD_FILE")" \
    -v "$TMP_PASS:/work" \
    "$IMAGE" \
    -c 'mosquitto_passwd -b -c /work/passwd "$MQTT_USERNAME" "$MQTT_PASSWORD"'
  mv -f "$TMP_PASS/passwd" "$PASSWD_FILE"
  rmdir "$TMP_PASS"
  docker run --rm --entrypoint sh -v "$PASSWD_FILE:/passwd" "$IMAGE" \
    -c 'chown 1883:1883 /passwd && chmod 600 /passwd'
fi

echo "MQTT TLS siap"
echo "  host     : $MQTT_HOSTNAME"
echo "  port     : 8883"
echo "  username : $MQTT_USERNAME"
echo "  password : $PASSWORD_FILE"
echo "  cert     : $CERT_DIR/server.crt"
