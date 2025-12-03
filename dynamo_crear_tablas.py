import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv
import os
import time

load_dotenv()

aws_region = os.getenv("REGION")

# dynamodb = boto3.client(
#     'dynamodb',
#     region_name=aws_region,
#     aws_access_key_id=os.getenv("ACCESS_KEY"),
#     aws_secret_access_key=os.getenv("SECRET_KEY"),
#     aws_session_token=os.getenv("SESSION_TOKEN")
# )

session = boto3.session.Session(
aws_access_key_id=os.getenv("ACCESS_KEY"),
aws_secret_access_key=os.getenv("SECRET_KEY"),
aws_session_token=os.getenv("SESSION_TOKEN"),
region_name=os.getenv("REGION"))


dynamodb = session.client('dynamodb')

#-----------------------------
# Eliminar tablas si existen
#-----------------------------

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

#----------------------------- 
# Crear tablas en DynamoDB
#-----------------------------
print("Creando tablas en DynamoDB...")

# Definición de tablas con Partition Key
tablas = {
    "CentrosEducativos": {
        "KeySchema": [{"AttributeName": "id_centro", "KeyType": "HASH"}],
        "AttributeDefinitions": [{"AttributeName": "id_centro", "AttributeType": "N"}],
        "ProvisionedThroughput": {"ReadCapacityUnits": 5, "WriteCapacityUnits": 5}
    },
    "Empresas": {
        "KeySchema": [{"AttributeName": "id_empresa", "KeyType": "HASH"}],
        "AttributeDefinitions": [{"AttributeName": "id_empresa", "AttributeType": "N"}],
        "ProvisionedThroughput": {"ReadCapacityUnits": 5, "WriteCapacityUnits": 5}
    },
    "Estudiantes": {
        "KeySchema": [{"AttributeName": "id_estudiante", "KeyType": "HASH"}],
        "AttributeDefinitions": [{"AttributeName": "id_estudiante", "AttributeType": "N"}],
        "ProvisionedThroughput": {"ReadCapacityUnits": 5, "WriteCapacityUnits": 5}
    },
    "Tutores": {
        "KeySchema": [{"AttributeName": "id_tutor", "KeyType": "HASH"}],
        "AttributeDefinitions": [{"AttributeName": "id_tutor", "AttributeType": "N"}],
        "ProvisionedThroughput": {"ReadCapacityUnits": 5, "WriteCapacityUnits": 5}
    },
    "Convenios": {
        "KeySchema": [{"AttributeName": "id_convenio", "KeyType": "HASH"}],
        "AttributeDefinitions": [{"AttributeName": "id_convenio", "AttributeType": "N"}],
        "ProvisionedThroughput": {"ReadCapacityUnits": 5, "WriteCapacityUnits": 5}
    },
    "Practicas": {
        "KeySchema": [{"AttributeName": "id_practica", "KeyType": "HASH"}],
        "AttributeDefinitions": [{"AttributeName": "id_practica", "AttributeType": "N"}],
        "ProvisionedThroughput": {"ReadCapacityUnits": 5, "WriteCapacityUnits": 5}
    },
    "RegistrosActividad": {
        "KeySchema": [{"AttributeName": "id_registro", "KeyType": "HASH"}],
        "AttributeDefinitions": [{"AttributeName": "id_registro", "AttributeType": "N"}],
        "ProvisionedThroughput": {"ReadCapacityUnits": 5, "WriteCapacityUnits": 5}
    },
    "Evaluaciones": {
        "KeySchema": [{"AttributeName": "id_evaluacion", "KeyType": "HASH"}],
        "AttributeDefinitions": [{"AttributeName": "id_evaluacion", "AttributeType": "N"}],
        "ProvisionedThroughput": {"ReadCapacityUnits": 5, "WriteCapacityUnits": 5}
    },
    "Incidencias": {
        "KeySchema": [{"AttributeName": "id_incidencia", "KeyType": "HASH"}],
        "AttributeDefinitions": [{"AttributeName": "id_incidencia", "AttributeType": "N"}],
        "ProvisionedThroughput": {"ReadCapacityUnits": 5, "WriteCapacityUnits": 5}
    },
    "Documentos": {
        "KeySchema": [{"AttributeName": "id_documento", "KeyType": "HASH"}],
        "AttributeDefinitions": [{"AttributeName": "id_documento", "AttributeType": "N"}],
        "ProvisionedThroughput": {"ReadCapacityUnits": 5, "WriteCapacityUnits": 5}
    },
    "IndicadoresAnaliticos": {
        "KeySchema": [{"AttributeName": "id_indicador", "KeyType": "HASH"}],
        "AttributeDefinitions": [{"AttributeName": "id_indicador", "AttributeType": "N"}],
        "ProvisionedThroughput": {"ReadCapacityUnits": 5, "WriteCapacityUnits": 5}
    },
    "LogsSistema": {
        "KeySchema": [{"AttributeName": "id_log", "KeyType": "HASH"}],
        "AttributeDefinitions": [{"AttributeName": "id_log", "AttributeType": "N"}],
        "ProvisionedThroughput": {"ReadCapacityUnits": 5, "WriteCapacityUnits": 5}
    }
}

# Crear tablas
for nombre_tabla, config in tablas.items():
    try:
        print(f"Creando tabla {nombre_tabla}...")
        dynamodb.create_table(TableName=nombre_tabla, **config)
        waiter = dynamodb.get_waiter('table_exists')
        waiter.wait(TableName=nombre_tabla)
        print(f"Tabla {nombre_tabla} creada correctamente")
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceInUseException':
            print(f"Tabla {nombre_tabla} ya existe")
        else:
            raise

print("Todas las tablas fueron creadas correctamente.")

#-----------------------------
# Crear al menos 3 tablas:
# 1. Una sin índice local ni global
# 1. Una con índice local
# 2. Una con índice global
#-----------------------------

dynamodb = session.resource('dynamodb')

# Tabla simple
try:
    dynamodb.create_table(
        TableName="TablaSimple",
        KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "N"}],
        ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5}
    )
    print("TablaSimple creada correctamente")
except:
    print("TablaSimple ya existe")
    
# Tabla con LSI
try:
    dynamodb.create_table(
        TableName="TablaConLSI",
        KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}, {"AttributeName": "fecha", "KeyType": "RANGE"}],
        AttributeDefinitions=[
            {"AttributeName": "id", "AttributeType": "N"},
            {"AttributeName": "fecha", "AttributeType": "S"},
            {"AttributeName": "estado", "AttributeType": "S"}  # atributo para el LSI
        ],
        ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
        LocalSecondaryIndexes=[
            {
                "IndexName": "EstadoIndex",
                "KeySchema":[
                    {"AttributeName": "id", "KeyType": "HASH"},
                    {"AttributeName": "estado", "KeyType": "RANGE"}
                ],
                "Projection":{"ProjectionType": "ALL"}
            }
        ]
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
            {"AttributeName": "correo", "AttributeType": "S"}  # atributo para el GSI
        ],
        ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
        GlobalSecondaryIndexes=[
            {
                "IndexName": "CorreoIndex",
                "KeySchema":[
                    {"AttributeName": "correo", "KeyType": "HASH"}
                ],
                "Projection":{"ProjectionType": "ALL"},
                "ProvisionedThroughput":{"ReadCapacityUnits": 5, "WriteCapacityUnits": 5}
            }
        ]
    )
    print("TablaConGSI creada correctamente")
except:
    print("TablaConGSI ya existe")

print("Todas 3 tablas de ejemplo fueron creadas correctamente.")

#-----------------------------
# Insertar 3 registros en cada tabla creada
#-----------------------------

tabla_simple = dynamodb.Table("TablaSimple")
registros_simple = [
    {"id": 1, "nombre": "Registro 1"},
    {"id": 2, "nombre": "Registro 2"},
    {"id": 3, "nombre": "Registro 3"}
]

# for item in registros_simple:
#     tabla_simple.put_item(Item=item)

print("3 registros insertados en TablaSimple")

tabla_lsi = dynamodb.Table("TablaConLSI")
registros_lsi = [
    {"id": 1, "fecha": "2025-01-01", "estado": "pendiente"},
    {"id": 1, "fecha": "2025-01-02", "estado": "en curso"},
    {"id": 2, "fecha": "2025-01-01", "estado": "finalizada"}
]

# for item in registros_lsi:
#     tabla_lsi.put_item(Item=item)

print("3 registros insertados en TablaConLSI")

tabla_gsi = dynamodb.Table("TablaConGSI")
registros_gsi = [
    {"id": 1, "correo": "usuario1@ejemplo.com", "nombre": "Usuario 1"},
    {"id": 2, "correo": "usuario2@ejemplo.com", "nombre": "Usuario 2"},
    {"id": 3, "correo": "usuario3@ejemplo.com", "nombre": "Usuario 3"}
]

# for item in registros_gsi:
#     tabla_gsi.put_item(Item=item)

print("3 registros insertados en TablaConGSI")

#-----------------------------
# Obtener un registro de cada tabla
#-----------------------------

response_simple = tabla_simple.get_item(Key={"id": 1})
print("Registro obtenido de TablaSimple:", response_simple.get('Item'))

response_lsi = tabla_lsi.get_item(Key={"id": 1, "fecha": "2025-01-01"})
print("Registro obtenido de TablaConLSI:", response_lsi.get('Item'))

response_gsi = tabla_gsi.get_item(Key={"id": 1})
print("Registro obtenido de TablaConGSI:", response_gsi.get('Item'))