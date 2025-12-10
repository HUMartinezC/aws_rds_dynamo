import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv
import os
import mysql.connector

load_dotenv()

session = boto3.session.Session(
    aws_access_key_id=os.getenv("ACCESS_KEY"),
    aws_secret_access_key=os.getenv("SECRET_KEY"),
    aws_session_token=os.getenv("SESSION_TOKEN"),
    region_name=os.getenv("REGION")
)

rds = session.client('rds')

db_identifier = os.getenv("DB_INSTANCE_ID")

try:
    rds.create_db_instance(
        DBInstanceIdentifier=db_identifier,
        AllocatedStorage=20,
        DBInstanceClass="db.t4g.micro",
        Engine="mariadb",
        MasterUsername=os.getenv("DB_USER"),
        MasterUserPassword=os.getenv("DB_PASSWORD"),
        PubliclyAccessible=True
    )

    print(f"Creando la base de datos '{db_identifier}', esto puede tardar unos minutos...")

    waiter = rds.get_waiter('db_instance_available')
    waiter.wait(DBInstanceIdentifier=db_identifier)

except ClientError as e:
    if "DBInstanceAlreadyExists" in str(e):
        print(f"La base de datos '{db_identifier}' ya existe.")
    else:
        raise

# Obtener la información de la base de datos
info = rds.describe_db_instances(DBInstanceIdentifier=db_identifier)
endpoint = info['DBInstances'][0]['Endpoint']['Address']

print("La base de datos está disponible en la siguiente endpoint:")
print(endpoint)
