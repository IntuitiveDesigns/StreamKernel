#!/bin/bash

set -e

echo "🔐 Generating StreamKernel SSL Certificates..."

# --- CONFIGURATION ---
PASSWORD="changeit"
VALIDITY=365
OU="StreamKernel Engineering"
LOCATION="Orlando"
STATE="FL"
COUNTRY="US"

# Clean up previous run
rm -rf secrets
mkdir -p secrets
cd secrets

echo "--> 1. Creating a Certificate Authority (CA)"
# Generate CA Private Key and Certificate
openssl req -new -x509 -keyout ca-key -out ca-cert -days $VALIDITY -nodes \
  -subj "/C=$COUNTRY/ST=$STATE/L=$LOCATION/O=$OU/CN=StreamKernel-CA"

echo "--> 2. Creating Server (Kafka Broker) Certs"
# Generate Server Keystore
keytool -genkey -noprompt \
  -alias server \
  -dname "CN=localhost, OU=$OU, O=$OU, L=$LOCATION, ST=$STATE, C=$COUNTRY" \
  -keystore kafka.server.keystore.jks \
  -keyalg RSA -storepass $PASSWORD -keypass $PASSWORD

# Create Server Certificate Signing Request (CSR)
keytool -certreq -alias server -file server-cert-file \
  -keystore kafka.server.keystore.jks -storepass $PASSWORD

# Sign the Server CSR with our CA
openssl x509 -req -CA ca-cert -CAkey ca-key -in server-cert-file -out server-cert-signed \
  -days $VALIDITY -CAcreateserial -passin pass:$PASSWORD

# Import CA into Server Keystore
keytool -import -noprompt -alias ca-root -file ca-cert \
  -keystore kafka.server.keystore.jks -storepass $PASSWORD

# Import Signed Server Cert into Server Keystore
keytool -import -noprompt -alias server -file server-cert-signed \
  -keystore kafka.server.keystore.jks -storepass $PASSWORD

echo "--> 3. Creating Client (StreamKernel App) Certs"
# Generate Client Keystore
keytool -genkey -noprompt \
  -alias client \
  -dname "CN=StreamKernel-App, OU=$OU, O=$OU, L=$LOCATION, ST=$STATE, C=$COUNTRY" \
  -keystore kafka.client.keystore.jks \
  -keyalg RSA -storepass $PASSWORD -keypass $PASSWORD

# Create Client Certificate Signing Request (CSR)
keytool -certreq -alias client -file client-cert-file \
  -keystore kafka.client.keystore.jks -storepass $PASSWORD

# Sign the Client CSR with our CA
openssl x509 -req -CA ca-cert -CAkey ca-key -in client-cert-file -out client-cert-signed \
  -days $VALIDITY -CAcreateserial -passin pass:$PASSWORD

# Import CA into Client Keystore
keytool -import -noprompt -alias ca-root -file ca-cert \
  -keystore kafka.client.keystore.jks -storepass $PASSWORD

# Import Signed Client Cert into Client Keystore
keytool -import -noprompt -alias client -file client-cert-signed \
  -keystore kafka.client.keystore.jks -storepass $PASSWORD

echo "--> 4. Creating Truststore (Shared)"
# Both Server and Client trust the CA, so we import the CA cert into a truststore
keytool -import -noprompt -alias ca-root -file ca-cert \
  -keystore kafka.truststore.jks -storepass $PASSWORD

echo "--> 5. Cleanup"
rm *-cert-file *-cert-signed *-key *.srl

echo "✅ DONE! Keys are in the 'secrets/' folder."
echo "   - kafka.server.keystore.jks (Mount to Broker)"
echo "   - kafka.client.keystore.jks (Mount to StreamKernel)"
echo "   - kafka.truststore.jks      (Mount to BOTH)"