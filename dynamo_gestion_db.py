import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError
from dotenv import load_dotenv
import os
import time

load_dotenv()

aws_region = os.getenv("REGION")

session = boto3.session.Session(
    aws_access_key_id=os.getenv("ACCESS_KEY"),
    aws_secret_access_key=os.getenv("SECRET_KEY"),
    aws_session_token=os.getenv("SESSION_TOKEN"),
    region_name=os.getenv("REGION"),
)


dynamodb = session.client("dynamodb")

# -----------------------------
# Eliminar tablas si existen
# -----------------------------

# Lista de todas las tablas
# tablas = [
#     "CentrosEducativos",
#     "Empresas",
#     "Estudiantes",
#     "Tutores",
#     "Convenios",
#     "Practicas",
#     "RegistrosActividad",
#     "Evaluaciones",
#     "Incidencias",
#     "Documentos",
#     "IndicadoresAnaliticos",
#     "LogsSistema",
#     "TablaSimple",
#     "TablaConLSI",
#     "TablaConGSI"
# ]

# Eliminar tablas si existen
# for tabla in tablas:
#     try:
#         dynamodb.delete_table(TableName=tabla)
#         print(f"Eliminando {tabla}...")
#         waiter = dynamodb.get_waiter('table_not_exists')
#         waiter.wait(TableName=tabla)
#         print(f"{tabla} eliminada correctamente")
#     except ClientError as e:
#         if e.response['Error']['Code'] == 'ResourceNotFoundException':
#             print(f"{tabla} no existe, nada que eliminar")
#         else:
#             raise

# -----------------------------
# Crear tablas en DynamoDB
# -----------------------------
print("---------- Creando tablas en DynamoDB... ----------\n")

# Definición de tablas con Partition Key
tablas = {
    "CentrosEducativos": {
        "KeySchema": [{"AttributeName": "id_centro", "KeyType": "HASH"}],
        "AttributeDefinitions": [{"AttributeName": "id_centro", "AttributeType": "N"}],
        "ProvisionedThroughput": {"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
    },
    "Empresas": {
        "KeySchema": [{"AttributeName": "id_empresa", "KeyType": "HASH"}],
        "AttributeDefinitions": [{"AttributeName": "id_empresa", "AttributeType": "N"}],
        "ProvisionedThroughput": {"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
    },
    "Estudiantes": {
        "KeySchema": [{"AttributeName": "id_estudiante", "KeyType": "HASH"}],
        "AttributeDefinitions": [
            {"AttributeName": "id_estudiante", "AttributeType": "N"}
        ],
        "ProvisionedThroughput": {"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
    },
    "Tutores": {
        "KeySchema": [{"AttributeName": "id_tutor", "KeyType": "HASH"}],
        "AttributeDefinitions": [{"AttributeName": "id_tutor", "AttributeType": "N"}],
        "ProvisionedThroughput": {"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
    },
    "Convenios": {
        "KeySchema": [{"AttributeName": "id_convenio", "KeyType": "HASH"}],
        "AttributeDefinitions": [
            {"AttributeName": "id_convenio", "AttributeType": "N"}
        ],
        "ProvisionedThroughput": {"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
    },
    "Practicas": {
        "KeySchema": [{"AttributeName": "id_practica", "KeyType": "HASH"}],
        "AttributeDefinitions": [
            {"AttributeName": "id_practica", "AttributeType": "N"}
        ],
        "ProvisionedThroughput": {"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
    },
    "RegistrosActividad": {
        "KeySchema": [{"AttributeName": "id_registro", "KeyType": "HASH"}],
        "AttributeDefinitions": [
            {"AttributeName": "id_registro", "AttributeType": "N"}
        ],
        "ProvisionedThroughput": {"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
    },
    "Evaluaciones": {
        "KeySchema": [{"AttributeName": "id_evaluacion", "KeyType": "HASH"}],
        "AttributeDefinitions": [
            {"AttributeName": "id_evaluacion", "AttributeType": "N"}
        ],
        "ProvisionedThroughput": {"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
    },
    "Incidencias": {
        "KeySchema": [{"AttributeName": "id_incidencia", "KeyType": "HASH"}],
        "AttributeDefinitions": [
            {"AttributeName": "id_incidencia", "AttributeType": "N"}
        ],
        "ProvisionedThroughput": {"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
    },
    "Documentos": {
        "KeySchema": [{"AttributeName": "id_documento", "KeyType": "HASH"}],
        "AttributeDefinitions": [
            {"AttributeName": "id_documento", "AttributeType": "N"}
        ],
        "ProvisionedThroughput": {"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
    },
    "IndicadoresAnaliticos": {
        "KeySchema": [{"AttributeName": "id_indicador", "KeyType": "HASH"}],
        "AttributeDefinitions": [
            {"AttributeName": "id_indicador", "AttributeType": "N"}
        ],
        "ProvisionedThroughput": {"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
    },
    "LogsSistema": {
        "KeySchema": [{"AttributeName": "id_log", "KeyType": "HASH"}],
        "AttributeDefinitions": [{"AttributeName": "id_log", "AttributeType": "N"}],
        "ProvisionedThroughput": {"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
    },
}

# Crear tablas
for nombre_tabla, config in tablas.items():
    try:
        print(f"Creando tabla {nombre_tabla}...")
        dynamodb.create_table(TableName=nombre_tabla, **config)
        waiter = dynamodb.get_waiter("table_exists")
        waiter.wait(TableName=nombre_tabla)
        print(f"Tabla {nombre_tabla} creada correctamente")
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceInUseException":
            print(f"Tabla {nombre_tabla} ya existe")
        else:
            raise

print("Todas las tablas fueron creadas correctamente.\n")

# -----------------------------
# Crear al menos 3 tablas:
# 1. Una sin índice local ni global
# 1. Una con índice local
# 2. Una con índice global
# -----------------------------

print("---------- Creando 3 tablas de ejemplo en DynamoDB... ----------\n")

dynamodb = session.resource("dynamodb")

# Tabla simple
try:
    dynamodb.create_table(
        TableName="TablaSimple",
        KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "N"}],
        ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
    )
    print("TablaSimple creada correctamente")
except:
    print("TablaSimple ya existe")

# Tabla con LSI
try:
    dynamodb.create_table(
        TableName="TablaConLSI",
        KeySchema=[
            {"AttributeName": "id", "KeyType": "HASH"},
            {"AttributeName": "fecha", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "id", "AttributeType": "N"},
            {"AttributeName": "fecha", "AttributeType": "S"},
            {"AttributeName": "estado", "AttributeType": "S"},  # atributo para el LSI
        ],
        ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
        LocalSecondaryIndexes=[
            {
                "IndexName": "EstadoIndex",
                "KeySchema": [
                    {"AttributeName": "id", "KeyType": "HASH"},
                    {"AttributeName": "estado", "KeyType": "RANGE"},
                ],
                "Projection": {"ProjectionType": "ALL"},
            }
        ],
    )
    print("TablaConLSI creada correctamente")
except:
    print("TablaConLSI ya existe")


# Tabla con GSI
try:
    dynamodb.create_table(
        TableName="TablaConGSI",
        KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
        AttributeDefinitions=[
            {"AttributeName": "id", "AttributeType": "N"},
            {"AttributeName": "correo", "AttributeType": "S"},  # atributo para el GSI
        ],
        ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
        GlobalSecondaryIndexes=[
            {
                "IndexName": "CorreoIndex",
                "KeySchema": [{"AttributeName": "correo", "KeyType": "HASH"}],
                "Projection": {"ProjectionType": "ALL"},
                "ProvisionedThroughput": {
                    "ReadCapacityUnits": 5,
                    "WriteCapacityUnits": 5,
                },
            }
        ],
    )
    print("TablaConGSI creada correctamente")
except:
    print("TablaConGSI ya existe")

print("\nTodas 3 tablas de ejemplo fueron creadas correctamente.\n")

# -----------------------------
# Insertar 3 registros en cada tabla creada
# -----------------------------

print("---------- Insertando 3 registros en cada tabla de ejemplo... ----------\n")

tabla_simple = dynamodb.Table("TablaSimple")
registros_simple = [
    {"id": 1, "nombre": "Registro 1"},
    {"id": 2, "nombre": "Registro 2"},
    {"id": 3, "nombre": "Registro 3"},
    {"id": 4, "nombre": "Registro 4"},
    {"id": 5, "nombre": "Registro 5"},
    {"id": 6, "nombre": "Registro 6"},
]

for item in registros_simple:
    tabla_simple.put_item(Item=item)

print("3 registros insertados en TablaSimple")

tabla_lsi = dynamodb.Table("TablaConLSI")
registros_lsi = [
    {"id": 1, "fecha": "2025-01-01", "estado": "pendiente"},
    {"id": 1, "fecha": "2025-01-02", "estado": "en curso"},
    {"id": 3, "fecha": "2025-01-01", "estado": "finalizada"},
    {"id": 4, "fecha": "2025-01-03", "estado": "pendiente"},
    {"id": 5, "fecha": "2025-01-01", "estado": "en curso"},
    {"id": 6, "fecha": "2025-01-04", "estado": "finalizada"},
]

for item in registros_lsi:
    tabla_lsi.put_item(Item=item)

print("3 registros insertados en TablaConLSI")

tabla_gsi = dynamodb.Table("TablaConGSI")
registros_gsi = [
    {"id": 1, "correo": "usuario1@ejemplo.com", "nombre": "Usuario 1", "fecha_registro": "2025-01-01"},
    {"id": 2, "correo": "usuario2@ejemplo.com", "nombre": "Usuario 2", "fecha_registro": "2025-02-02"},
    {"id": 3, "correo": "usuario3@ejemplo.com", "nombre": "Usuario 3", "fecha_registro": "2025-03-03"},
    {"id": 4, "correo": "usuario4@ejemplo.com", "nombre": "Usuario 4", "fecha_registro": "2025-04-04"},
    {"id": 5, "correo": "usuario5@ejemplo.com", "nombre": "Usuario 5", "fecha_registro": "2025-05-05"},
    {"id": 6, "correo": "usuario6@ejemplo.com", "nombre": "Usuario 6", "fecha_registro": "2025-06-06"},
]

for item in registros_gsi:
    tabla_gsi.put_item(Item=item)

print("3 registros insertados en TablaConGSI\n")

# -----------------------------
# Obtener un registro de cada tabla
# -----------------------------

print("---------- Obteniendo un registro de cada tabla... ----------\n")

response_simple = tabla_simple.get_item(Key={"id": 1})
print("Registro obtenido de TablaSimple:", response_simple.get("Item"))

response_lsi = tabla_lsi.get_item(Key={"id": 1, "fecha": "2025-01-01"})
print("Registro obtenido de TablaConLSI:", response_lsi.get("Item"))

response_gsi = tabla_gsi.get_item(Key={"id": 1})
print("Registro obtenido de TablaConGSI:", response_gsi.get("Item"))


# -----------------------------
# Actualizar un registro de cada tabla
# -----------------------------

print("\n---------- Actualizando un registro de cada tabla... ----------\n")

tabla_simple.update_item(
    Key={"id": 1},
    UpdateExpression="SET nombre = :nuevo_nombre",
    ExpressionAttributeValues={":nuevo_nombre": "Registro 1 Actualizado"},
)

print("Registro actualizado en TablaSimple")

tabla_lsi.update_item(
    Key={"id": 1, "fecha": "2025-01-01"},
    UpdateExpression="SET estado = :nuevo_estado",
    ExpressionAttributeValues={":nuevo_estado": "completado"},
)

print("Registro actualizado en TablaConLSI")

tabla_gsi.update_item(
    Key={"id": 1},
    UpdateExpression="SET nombre = :nuevo_correo",
    ExpressionAttributeValues={":nuevo_correo": "usuario1@example.org"},
)

print("Registro actualizado en TablaConGSI")


# -----------------------------
# Eliminar un registro de cada tabla
# -----------------------------

print("\n---------- Eliminando un registro de cada tabla... ----------\n")

tabla_simple.delete_item(Key={"id": 1})
print("Registro eliminado de TablaSimple")

tabla_lsi.delete_item(Key={"id": 1, "fecha": "2025-01-01"})
print("Registro eliminado de TablaConLSI")

tabla_gsi.delete_item(Key={"id": 1})
print("Registro eliminado de TablaConGSI\n")

# -----------------------------
# Obtener todos los registros de cada tabla
# -----------------------------

print("---------- Obteniendo todos los registros de cada tabla... ----------\n")

response_simple = tabla_simple.scan()
print("Registros en TablaSimple:", response_simple.get("Items"))

response_lsi = tabla_lsi.scan()
print("Registros en TablaConLSI:", response_lsi.get("Items"))

response_gsi = tabla_gsi.scan()
print("Registros en TablaConGSI:", response_gsi.get("Items"))

# -----------------------------
# Obtener una conjunto de registros de un filtrado de cada tabla usando el scan. En la global utilizar el índice global para ello.
# -----------------------------

print("\n---------- Filtrando registros en cada tabla... ----------\n")

response_simple = tabla_simple.scan(
    FilterExpression="nombre = :nombre_valor",
    ExpressionAttributeValues={":nombre_valor": "Registro 2"},
)
print("Registros filtrados en TablaSimple:", response_simple.get("Items"))

response_lsi = tabla_lsi.scan(
    FilterExpression="estado = :estado_valor",
    ExpressionAttributeValues={":estado_valor": "en curso"},
)

print("Registros filtrados en TablaConLSI:", response_lsi.get("Items"))

response_gsi = tabla_gsi.scan(
    IndexName="CorreoIndex",
    FilterExpression="correo = :correo_valor",
    ExpressionAttributeValues={":correo_valor": "usuario3@ejemplo.com"},
)

print("Registros filtrados en TablaConGSI:", response_gsi.get("Items"))

# -----------------------------
# Realizar una eliminación condicional de cada tabla. En la global utilizar el índice global para ello.
# -----------------------------

print("\n---------- Eliminación condicional en cada tabla... ----------\n")

try:
    tabla_simple.delete_item(
        Key={"id": 2},
        ConditionExpression="nombre = :nombre_valor",
        ExpressionAttributeValues={":nombre_valor": "Registro 2"},
    )
    print("Eliminación condicional exitosa en TablaSimple")
except ClientError as e:
    if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
        print("Condición no cumplida para eliminación en TablaSimple")


try:
    tabla_lsi.delete_item(
        Key={"id": 1, "fecha": "2025-01-02"},
        ConditionExpression="estado = :estado_valor",
        ExpressionAttributeValues={":estado_valor": "en curso"},
    )
    print("Eliminación condicional exitosa en TablaConLSI")
except ClientError as e:
    if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
        print("Condición no cumplida para eliminación en TablaConLSI")

try:
    # 1. Buscar el ítem usando el GSI
    response = tabla_gsi.query(
        IndexName="CorreoIndex",
        KeyConditionExpression=Key("correo").eq("usuario2@ejemplo.com"),
    )

    items = response.get("Items", [])

    if items:
        # 2. Eliminar usando la clave primaria encontrada
        for item in items:
            tabla_gsi.delete_item(
                Key={"id": item["id"]},  # Asumiendo que "id" es la PK
                ConditionExpression="correo = :correo_valor",
                ExpressionAttributeValues={":correo_valor": "usuario2@ejemplo.com"},
            )
        print("Eliminación condicional exitosa en TablaConGSI")
    else:
        print("No se encontró el registro con ese correo")

except ClientError as e:
    if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
        print("Condición no cumplida para eliminación")


print("\nRegistros condicionales eliminados correctamente de cada tabla.\n")

# -----------------------------
# Obtener un conjunto de datos a través de varios filtros aplicado en cada tabla.
# -----------------------------

print("---------- Filtrado avanzado en cada tabla... ----------\n")

# ---------------- TablaSimple ----------------
response_simple = tabla_simple.scan(
    FilterExpression="contains(nombre, :nombre_parcial) AND id > :id_val",
    ExpressionAttributeValues={
        ":nombre_parcial": "Registro",
        ":id_val": 1
    }
)
print("Filtrado avanzado TablaSimple:", response_simple.get("Items"))

# ---------------- TablaConLSI ----------------
response_lsi = tabla_lsi.scan(
    FilterExpression="estado = :estado1 OR estado = :estado2",
    ExpressionAttributeValues={
        ":estado1": "en curso",
        ":estado2": "pendiente"
    }
)
print("Filtrado avanzado TablaConLSI:", response_lsi.get("Items"))

# ---------------- TablaConGSI ----------------
# Ejemplo: filtrar por correo usando el índice global y fecha mayor a cierta fecha
from boto3.dynamodb.conditions import Key, Attr

response_gsi = tabla_gsi.query(
    IndexName="CorreoIndex",
    KeyConditionExpression=Key("correo").eq("usuario3@ejemplo.com"),
    FilterExpression=Attr("fecha_registro").gte("2025-03-03")
)
print("Filtrado avanzado TablaConGSI:", response_gsi.get("Items"))

# -----------------------------
# Operaciones con PartiQL en cada tabla.
# -----------------------------

client = session.client("dynamodb")

print("---------- Operaciones con PartiQL en cada tabla ----------\n")

response_simple = client.execute_statement(
    Statement="SELECT * FROM TablaSimple WHERE nombre='Registro 6'"
)
print("PartiQL - TablaSimple:", response_simple.get("Items"))

response_lsi = client.execute_statement(
    Statement="SELECT * FROM TablaConLSI WHERE estado='en curso'"
)
print("PartiQL - TablaConLSI:", response_lsi.get("Items"))

response_gsi = client.execute_statement(
    Statement="""
        SELECT * FROM TablaConGSI.CorreoIndex 
        WHERE correo='usuario3@ejemplo.com'
    """
)
print("PartiQL - TablaConGSI:", response_gsi.get("Items"))