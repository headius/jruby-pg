#!/usr/bin/env bash

if [ ! -f server.crt.der ]; then
    ./generate_keys.sh
fi

sudo keytool -storepass changeit \
    -keystore $JAVA_HOME/jre/lib/security/cacerts \
    -delete \
    -alias postgresql

sudo keytool -storepass changeit \
    -keystore $JAVA_HOME/jre/lib/security/cacerts \
    -alias postgresql \
    -noprompt \
    -import -file server.crt.der
