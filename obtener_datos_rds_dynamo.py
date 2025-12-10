import mysql.connector
import boto3
from decimal import Decimal
import json
import os
from dotenv import load_dotenv
import datetime

load_dotenv()

# -----------------------------
# Función para serializar Decimals
# -----------------------------
def decimal_default(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    elif isinstance(obj, (datetime.date, datetime.datetime)):
        return obj.isoformat()  # Convierte a 'YYYY-MM-DD' o ISO completo
    return str(obj)  # Por si hay otros tipos no serializables

# -----------------------------
# Conexión a RDS MySQL
# -----------------------------
DB_HOST = os.getenv("DB_ENDPOINT")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_PORT = int(os.getenv("DB_PORT", 3306))
DB_NAME = os.getenv("DB_INSTANCE_ID")

try:
    rds_conn = mysql.connector.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        port=DB_PORT,
        database=DB_NAME
    )
    rds_cursor = rds_conn.cursor(dictionary=True)
    print("Conexión a RDS establecida")
except mysql.connector.Error as err:
    print(f"Error al conectar a RDS: {err}")
    exit(1)

# -----------------------------
# Conexión a DynamoDB
# -----------------------------
session = boto3.session.Session(
    aws_access_key_id=os.getenv("ACCESS_KEY"),
    aws_secret_access_key=os.getenv("SECRET_KEY"),
    aws_session_token=os.getenv("SESSION_TOKEN"),
    region_name=os.getenv("REGION")
)

dynamodb = session.resource("dynamodb")
print("Conexión a DynamoDB establecida")

# -----------------------------
# Obtener datos de RDS (ejemplo: estudiantes)
# -----------------------------
rds_cursor.execute("SELECT * FROM ESTUDIANTES LIMIT 10;")
rds_estudiantes = rds_cursor.fetchall()

# -----------------------------
# Obtener datos de DynamoDB (ejemplo: TablaSimple)
# -----------------------------
tabla_simple = dynamodb.Table("TablaSimple")
dynamo_simple = tabla_simple.scan()
dynamo_simple_items = dynamo_simple.get("Items", [])

# -----------------------------
# Combinar datos
# -----------------------------
datos_combinados = {
    "RDS_ESTUDIANTES": rds_estudiantes,
    "DYNAMO_TABLA_SIMPLE": dynamo_simple_items
}

# -----------------------------
# Guardar en JSON
# -----------------------------
with open("datos_combinados.json", "w", encoding="utf-8") as f:
    json.dump(datos_combinados, f, default=decimal_default, ensure_ascii=False, indent=4)

print("Archivo 'datos_combinados.json' creado correctamente")

# -----------------------------
# Cerrar conexiones
# -----------------------------
rds_cursor.close()
rds_conn.close()
print("Conexión a RDS cerrada")