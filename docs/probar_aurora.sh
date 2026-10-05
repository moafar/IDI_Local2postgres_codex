#!/usr/bin/env bash
# Ejecutar con:
# bash docs/probar_aurora.sh

HOST="bi-idi.cluster-cvq28ma6cvue.eu-west-1.rds.amazonaws.com"
PORT="5432"
DB="postgres"
USER="bi_admin"

echo "========================================"
echo " Verificación de conexión a AWS Aurora"
echo "========================================"
echo

# ------------------------------------------------------------
# PASO 1: Resolución DNS
# ------------------------------------------------------------
echo "[1/4] Verificando resolución DNS..."
echo "Ejecutando: getent ahostsv4 \"$HOST\""

IP=$(getent ahostsv4 "$HOST" | awk 'NR==1 {print $1}')

if [[ -z "$IP" ]]; then
    echo "ERROR: No se pudo resolver el host:"
    echo "  $HOST"
    exit 1
fi

echo "OK: El host resuelve correctamente."
echo "  Host: $HOST"
echo "  IP:   $IP"
echo

# ------------------------------------------------------------
# PASO 2: Conectividad TCP al puerto PostgreSQL
# ------------------------------------------------------------
echo "[2/4] Verificando conectividad TCP al puerto $PORT..."
echo "Ejecutando: nc -z -w 10 \"$HOST\" \"$PORT\""

if nc -z -w 5 "$HOST" "$PORT" >/dev/null 2>&1; then
    echo "OK: El puerto $PORT es accesible."
else
    echo "ERROR: No se puede establecer conexión TCP."
    echo "  Host: $HOST"
    echo "  IP:   $IP"
    echo "  Puerto: $PORT"
    echo
    echo "Posibles causas:"
    echo "  - VPN/Sophos no conectado"
    echo "  - problema de red"
    echo "  - reglas de firewall o Security Group"
    echo "  - endpoint de Aurora no accesible"
    exit 2
fi

echo

# ------------------------------------------------------------
# PASO 3: Comprobar que PostgreSQL acepta conexiones
# ------------------------------------------------------------
echo "[3/4] Verificando disponibilidad de PostgreSQL..."
echo "Ejecutando: pg_isready -h \"$HOST\" -p \"$PORT\" -d \"$DB\" -t 10"

PG_STATUS=$(pg_isready -h "$HOST" -p "$PORT" -d "$DB" -t 10 2>&1)
PG_EXIT=$?

echo "$PG_STATUS"

if [[ $PG_EXIT -ne 0 ]]; then
    echo
    echo "ERROR: El servidor responde por TCP, pero PostgreSQL no está aceptando conexiones."
    exit 3
fi

echo "OK: PostgreSQL está aceptando conexiones."
echo

# ------------------------------------------------------------
# PASO 4: Autenticación real con psql
# ------------------------------------------------------------
echo "[4/4] Verificando autenticación con el usuario '$USER'..."
echo

read -rsp "Contraseña: " PASSWORD
echo
echo

echo "Ejecutando: psql \"host=$HOST port=$PORT dbname=$DB user=$USER sslmode=require connect_timeout=10\" -tAc \"SELECT current_user, current_database(), version();\""
echo

PGPASSWORD="$PASSWORD" psql \
    "host=$HOST port=$PORT dbname=$DB user=$USER sslmode=require connect_timeout=10" \
    -tAc "SELECT current_user, current_database(), version();"

PSQL_EXIT=$?

unset PASSWORD
unset PGPASSWORD

echo

if [[ $PSQL_EXIT -eq 0 ]]; then
    echo "========================================"
    echo " RESULTADO: CONEXIÓN CORRECTA"
    echo "========================================"
    echo "DNS:        OK"
    echo "TCP $PORT:   OK"
    echo "PostgreSQL: OK"
    echo "Login:      OK"
else
    echo "========================================"
    echo " RESULTADO: ERROR DE CONEXIÓN"
    echo "========================================"
    echo "DNS:        OK"
    echo "TCP $PORT:   OK"
    echo "PostgreSQL: OK"
    echo "Login:      ERROR"
    echo
    echo "La red y Aurora responden correctamente."
    echo "El problema está probablemente en:"
    echo "  - usuario"
    echo "  - contraseña"
    echo "  - base de datos"
    echo "  - configuración SSL/autenticación"
fi

exit "$PSQL_EXIT"