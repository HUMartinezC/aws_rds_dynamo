# aws_rds_dynamo

Proyecto en Python para gestionar y sincronizar datos entre una base de datos en Amazon RDS y tablas en Amazon DynamoDB utilizando la librería Boto3.

## Contenido principal

- rds_gestion_db.py — Crea las tablas en RDS, inserta datos de ejemplo y realiza consultas básicas.
- dynamo_gestion_db.py — Crea las tablas necesarias en DynamoDB y realiza operaciones (crear, insertar, actualizar, consultar, eliminar).
- obtener_datos_rds_dynamo.py — Obtiene datos tanto de RDS como de DynamoDB y los combina en un JSON de salida (`datos_combinados.json`).
- rds_crear_bd.py — (script auxiliar) Para creación de la base de datos/estructura inicial en RDS.
- datos_combinados.json — Salida JSON con los datos combinados generados por `obtener_datos_rds_dynamo.py`.

## Requisitos

- Python 3.8+
- Instalar dependencias:
  - pip install -r requirements.txt

## Configuración (variables de entorno)

Todas las variables necesarias están en el fichero `.env_p`. Flujo recomendado:

1. Copiar `.env_p` a `.env`:
   - cp .env_p .env
2. Rellenar las variables con las credenciales y endpoints (RDS, AWS, etc.).
3. Los scripts cargan las variables desde `.env` (si se usa python-dotenv) o usan las variables de entorno del sistema.

## Uso — scripts principales

1. rds_gestion_db.py

   - Propósito: crear tablas en la base RDS, poblar con datos de ejemplo y ejecutar consultas básicas para verificar la instalación.
   - Ejecución:
     - python rds_gestion_db.py
   - Notas:
     - Asegúrate de que las variables de conexión a RDS en `.env` estén correctas (host, puerto, usuario, contraseña, nombre de BD).
     - El script realiza operaciones DDL y DML (creación de tablas e inserción de datos de prueba).

2. dynamo_gestion_db.py

   - Propósito: crear las tablas necesarias en DynamoDB y ejecutar operaciones CRUD básicas contra las mismas.
   - Ejecución:
     - python dynamo_gestion_db.py
   - Notas:
     - Requiere credenciales AWS con permisos para crear/editar tablas DynamoDB.
     - Comprueba los nombres de tabla y claves primarias en el script o en `.env`.

3. obtener_datos_rds_dynamo.py

   - Propósito: extraer datos de RDS y DynamoDB, combinar/transformar según la lógica implementada, y volcar el resultado en `datos_combinados.json`.
   - Ejecución:
     - python obtener_datos_rds_dynamo.py
   - Salida:
     - `datos_combinados.json` (ya incluido en el repo como ejemplo/fixture).
   - Notas:
     - Comprueba permisos y conectividad hacia ambos servicios.
     - El script se encarga de leer de ambas fuentes, combinar los registros y producir un JSON unificado.

4. rds_crear_bd.py
   - Propósito: auxiliar para crear la base de datos / esquema en RDS si fuera necesario de forma separada.
   - Ejecución:
     - python rds_crear_bd.py

## Flujo de trabajo típico

1. Configurar `.env` a partir de `.env_p`.
2. Ejecutar `rds_crear_bd.py` para crear la base de datos.
3. Ejecutar `rds_gestion_db.py` para crear tablas y poblar con datos de prueba.
4. Ejecutar `dynamo_gestion_db.py` para crear tablas y realizar operaciones en DynamoDB.
5. Ejecutar `obtener_datos_rds_dynamo.py` para extraer y combinar datos en `datos_combinados.json`.
6. Revisar `datos_combinados.json` como verificación del proceso.
