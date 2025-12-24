#!/bin/bash
set -euo pipefail

echo "🔐 Generating StreamKernel SSL Certificates..."

# --- CONFIGURATION ---
PASSWORD="changeit"
VALIDITY=365
OU="StreamKernel Engineering"
LOCATION="Orlando"
STATE="FL"
COUNTRY="US"
STORETYPE="PKCS12"   # Best practice: explicit and modern

rm -rf secrets
mkdir -p secrets
cd secrets

echo "--> 1. Creating a Certificate Authority (CA)"
openssl req -new -x509 -keyout ca-key -out ca-cert -days "$VALIDITY" -nodes \
  -subj "/C=$COUNTRY/ST=$STATE/L=$LOCATION/O=$OU/CN=StreamKernel-CA"

echo "--> 2. Creating Server (Kafka Broker) Certs with SAN"
# Create a SAN config file for OpenSSL signing
cat > server-ext.cnf <<EOF
subjectAltName = DNS:localhost,DNS:broker,IP:127.0.0.1
extendedKeyUsage = serverAuth
keyUsage = digitalSignature,keyEncipherment
EOF

# Generate Server Keystore
keytool -genkeypair -noprompt \
  -alias server \
  -dname "CN=localhost, OU=$OU, O=$OU, L=$LOCATION, ST=$STATE, C=$COUNTRY" \
  -keystore kafka.server.keystore.p12 \
  -storetype "$STORETYPE" \
  -keyalg RSA -keysize 2048 \
  -storepass "$PASSWORD" -keypass "$PASSWORD"

# CSR
keytool -certreq -alias server -file server.csr \
  -keystore kafka.server.keystore.p12 -storetype "$STORETYPE" -storepass "$PASSWORD"

# Sign CSR with CA including SAN
openssl x509 -req -in server.csr -CA ca-cert -CAkey ca-key -CAcreateserial \
  -out server-cert-signed.pem -days "$VALIDITY" -sha256 \
  -extfile server-ext.cnf

# Import CA and signed cert back into keystore
keytool -importcert -noprompt -alias ca-root -file ca-cert \
  -keystore kafka.server.keystore.p12 -storetype "$STORETYPE" -storepass "$PASSWORD"

keytool -importcert -noprompt -alias server -file server-cert-signed.pem \
  -keystore kafka.server.keystore.p12 -storetype "$STORETYPE" -storepass "$PASSWORD"

echo "--> 3. Creating Client (StreamKernel App) Certs"
cat > client-ext.cnf <<EOF
extendedKeyUsage = clientAuth
keyUsage = digitalSignature,keyEncipherment
EOF

keytool -genkeypair -noprompt \
  -alias client \
  -dname "CN=StreamKernel-App, OU=$OU, O=$OU, L=$LOCATION, ST=$STATE, C=$COUNTRY" \
  -keystore kafka.client.keystore.p12 \
  -storetype "$STORETYPE" \
  -keyalg RSA -keysize 2048 \
  -storepass "$PASSWORD" -keypass "$PASSWORD"

keytool -certreq -alias client -file client.csr \
  -keystore kafka.client.keystore.p12 -storetype "$STORETYPE" -storepass "$PASSWORD"

openssl x509 -req -in client.csr -CA ca-cert -CAkey ca-key -CAcreateserial \
  -out client-cert-signed.pem -days "$VALIDITY" -sha256 \
  -extfile client-ext.cnf

keytool -importcert -noprompt -alias ca-root -file ca-cert \
  -keystore kafka.client.keystore.p12 -storetype "$STORETYPE" -storepass "$PASSWORD"

keytool -importcert -noprompt -alias client -file client-cert-signed.pem \
  -keystore kafka.client.keystore.p12 -storetype "$STORETYPE" -storepass "$PASSWORD"

echo "--> 4. Creating Truststore (Shared)"
keytool -importcert -noprompt -alias ca-root -file ca-cert \
  -keystore kafka.truststore.p12 -storetype "$STORETYPE" -storepass "$PASSWORD"

echo "--> 5. Cleanup"
rm -f *.csr *-cert-signed.pem *.srl server-ext.cnf client-ext.cnf

echo "✅ DONE! Keys are in the 'secrets/' folder."
echo "   - kafka.server.keystore.p12 (Mount to Broker)"
echo "   - kafka.client.keystore.p12 (Mount to StreamKernel)"
echo "   - kafka.truststore.p12      (Mount to BOTH)"
