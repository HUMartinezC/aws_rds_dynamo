import mysql.connector
from mysql.connector import errorcode
from dotenv import load_dotenv
import os

load_dotenv()

# -----------------------------
# Variables globales
# -----------------------------
DB_HOST = os.getenv("DB_ENDPOINT")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_PORT = int(os.getenv("DB_PORT", 3306))
DB_NAME = os.getenv("DB_INSTANCE_ID")

connection = None
cursor = None

# -----------------------------
# Conectar a MySQL y seleccionar base de datos
# -----------------------------
try:
    connection = mysql.connector.connect(
        host=DB_HOST, user=DB_USER, password=DB_PASSWORD, port=DB_PORT
    )
    cursor = connection.cursor()
    print("Conexión establecida a MySQL RDS")

    # Crear base de datos si no existe
    cursor.execute(
        f"CREATE DATABASE IF NOT EXISTS `{DB_NAME}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"
    )
    cursor.execute(f"USE `{DB_NAME}`;")
    print(f"Base de datos '{DB_NAME}' lista")

except mysql.connector.Error as err:
    print(f"Error al conectar o crear la DB: {err}")
    exit(1)

# -----------------------------
# Diccionario de tablas
# -----------------------------
tablas = {}

tablas[
    "CENTROS_EDUCATIVOS"
] = """
CREATE TABLE IF NOT EXISTS CENTROS_EDUCATIVOS (
    id_centro INT AUTO_INCREMENT PRIMARY KEY,
    nombre VARCHAR(150) NOT NULL,
    direccion VARCHAR(255),
    provincia VARCHAR(100),
    tipo_centro VARCHAR(100)
);
"""

tablas[
    "EMPRESAS"
] = """
CREATE TABLE IF NOT EXISTS EMPRESAS (
    id_empresa INT AUTO_INCREMENT PRIMARY KEY,
    nombre VARCHAR(150) NOT NULL,
    sector VARCHAR(100),
    direccion VARCHAR(255),
    ciudad VARCHAR(100),
    persona_contacto VARCHAR(150),
    correo_contacto VARCHAR(150),
    telefono_contacto VARCHAR(20),
    satisfaccion_media DECIMAL(4,2)
);
"""

tablas[
    "ESTUDIANTES"
] = """
CREATE TABLE IF NOT EXISTS ESTUDIANTES (
    id_estudiante INT AUTO_INCREMENT PRIMARY KEY,
    dni VARCHAR(15) UNIQUE NOT NULL,
    nombre VARCHAR(150) NOT NULL,
    fecha_nacimiento DATE NOT NULL,
    correo VARCHAR(150),
    telefono VARCHAR(20),
    nacionalidad VARCHAR(50),
    id_centro INT NOT NULL,
    titulacion VARCHAR(150),
    curso_academico VARCHAR(20),
    FOREIGN KEY (id_centro) REFERENCES CENTROS_EDUCATIVOS(id_centro)
        ON UPDATE CASCADE ON DELETE CASCADE
);
"""

tablas[
    "TUTORES"
] = """
CREATE TABLE IF NOT EXISTS TUTORES (
    id_tutor INT AUTO_INCREMENT PRIMARY KEY,
    nombre VARCHAR(150) NOT NULL,
    correo VARCHAR(150),
    telefono VARCHAR(20),
    id_centro INT NOT NULL,
    especialidad VARCHAR(150),
    FOREIGN KEY (id_centro) REFERENCES CENTROS_EDUCATIVOS(id_centro)
        ON UPDATE CASCADE ON DELETE CASCADE
);
"""

tablas[
    "CONVENIOS"
] = """
CREATE TABLE IF NOT EXISTS CONVENIOS (
    id_convenio INT AUTO_INCREMENT PRIMARY KEY,
    id_empresa INT NOT NULL,
    id_centro INT NOT NULL,
    fecha_inicio DATE,
    fecha_fin DATE,
    observaciones TEXT,
    FOREIGN KEY (id_empresa) REFERENCES EMPRESAS(id_empresa)
        ON UPDATE CASCADE ON DELETE CASCADE,
    FOREIGN KEY (id_centro) REFERENCES CENTROS_EDUCATIVOS(id_centro)
        ON UPDATE CASCADE ON DELETE CASCADE
);
"""

tablas[
    "PRACTICAS"
] = """
CREATE TABLE IF NOT EXISTS PRACTICAS (
    id_practica INT AUTO_INCREMENT PRIMARY KEY,
    id_estudiante INT NOT NULL,
    id_tutor INT NOT NULL,
    id_empresa INT NOT NULL,
    id_convenio INT NOT NULL,
    fecha_inicio DATE,
    fecha_fin DATE,
    horas_totales INT,
    estado ENUM('pendiente', 'en curso', 'finalizada', 'cancelada') DEFAULT 'pendiente',
    evaluacion_final DECIMAL(4,2),
    FOREIGN KEY (id_estudiante) REFERENCES ESTUDIANTES(id_estudiante)
        ON UPDATE CASCADE ON DELETE CASCADE,
    FOREIGN KEY (id_tutor) REFERENCES TUTORES(id_tutor)
        ON UPDATE CASCADE ON DELETE CASCADE,
    FOREIGN KEY (id_empresa) REFERENCES EMPRESAS(id_empresa)
        ON UPDATE CASCADE ON DELETE CASCADE,
    FOREIGN KEY (id_convenio) REFERENCES CONVENIOS(id_convenio)
        ON UPDATE CASCADE ON DELETE CASCADE
);
"""

tablas[
    "REGISTROS_ACTIVIDAD"
] = """
CREATE TABLE IF NOT EXISTS REGISTROS_ACTIVIDAD (
    id_registro INT AUTO_INCREMENT PRIMARY KEY,
    id_practica INT NOT NULL,
    fecha DATE,
    descripcion TEXT,
    horas INT,
    validado_por_tutor BOOLEAN DEFAULT FALSE,
    FOREIGN KEY (id_practica) REFERENCES PRACTICAS(id_practica)
        ON UPDATE CASCADE ON DELETE CASCADE
);
"""

tablas[
    "EVALUACIONES"
] = """
CREATE TABLE IF NOT EXISTS EVALUACIONES (
    id_evaluacion INT AUTO_INCREMENT PRIMARY KEY,
    id_practica INT NOT NULL,
    tipo ENUM('inicial', 'intermedia', 'final'),
    fecha DATE,
    puntuacion DECIMAL(4,2),
    comentarios TEXT,
    FOREIGN KEY (id_practica) REFERENCES PRACTICAS(id_practica)
        ON UPDATE CASCADE ON DELETE CASCADE
);
"""

tablas[
    "INCIDENCIAS"
] = """
CREATE TABLE IF NOT EXISTS INCIDENCIAS (
    id_incidencia INT AUTO_INCREMENT PRIMARY KEY,
    id_practica INT NOT NULL,
    fecha DATE,
    descripcion TEXT,
    tipo ENUM('leve', 'moderada', 'grave'),
    resuelta BOOLEAN DEFAULT FALSE,
    FOREIGN KEY (id_practica) REFERENCES PRACTICAS(id_practica)
        ON UPDATE CASCADE ON DELETE CASCADE
);
"""

tablas[
    "DOCUMENTOS"
] = """
CREATE TABLE IF NOT EXISTS DOCUMENTOS (
    id_documento INT AUTO_INCREMENT PRIMARY KEY,
    id_practica INT NOT NULL,
    tipo ENUM('informe', 'evaluacion', 'anexo', 'otro'),
    nombre_archivo VARCHAR(255),
    ruta_archivo VARCHAR(255),
    fecha_subida DATETIME DEFAULT CURRENT_TIMESTAMP,
    tamano BIGINT,
    FOREIGN KEY (id_practica) REFERENCES PRACTICAS(id_practica)
        ON UPDATE CASCADE ON DELETE CASCADE
);
"""

tablas[
    "INDICADORES_ANALITICOS"
] = """
CREATE TABLE IF NOT EXISTS INDICADORES_ANALITICOS (
    id_indicador INT AUTO_INCREMENT PRIMARY KEY,
    id_empresa INT NOT NULL,
    id_centro INT NOT NULL,
    anio INT,
    practicas_realizadas INT,
    tasa_finalizacion DECIMAL(5,2),
    satisfaccion_media DECIMAL(4,2),
    tasa_contratacion DECIMAL(5,2),
    abandonos INT,
    FOREIGN KEY (id_empresa) REFERENCES EMPRESAS(id_empresa)
        ON UPDATE CASCADE ON DELETE CASCADE,
    FOREIGN KEY (id_centro) REFERENCES CENTROS_EDUCATIVOS(id_centro)
        ON UPDATE CASCADE ON DELETE CASCADE
);
"""

tablas[
    "LOGS_SISTEMA"
] = """
CREATE TABLE IF NOT EXISTS LOGS_SISTEMA (
    id_log BIGINT AUTO_INCREMENT PRIMARY KEY,
    id_usuario INT,
    tipo_evento VARCHAR(100),
    fecha_evento DATETIME DEFAULT CURRENT_TIMESTAMP,
    detalle TEXT,
    ip VARCHAR(45)
);
"""

# -----------------------------
# Crear tablas
# -----------------------------
for nombre, ddl in tablas.items():
    try:
        print(f"Creando tabla {nombre}...")
        cursor.execute(ddl)
    except mysql.connector.Error as err:
        print(f"Error creando tabla {nombre}: {err}")

connection.commit()
print("Todas las tablas fueron creadas correctamente.")

# -----------------------------
# Insertar datos de prueba
# -----------------------------
try:
    # Insertar CENTROS_EDUCATIVOS
    centros = [
        ("Instituto Tecnológico A", "Calle Falsa 123", "Madrid", "Público"),
        ("Colegio Profesional B", "Avenida Siempre Viva 456", "Barcelona", "Privado"),
    ]
    cursor.executemany(
        "INSERT INTO CENTROS_EDUCATIVOS (nombre, direccion, provincia, tipo_centro) VALUES (%s, %s, %s, %s);",
        centros,
    )

    # Insertar EMPRESAS
    empresas = [
        (
            "Tech Solutions S.A.",
            "Tecnología",
            "Calle Innovación 789",
            "Madrid",
            "Laura Gómez",
            "laura.gomez@tech.com",
            None,
        ),
        (
            "Servicios Globales S.L.",
            "Consultoría",
            "Avenida Empresarial 101",
            "Barcelona",
            "Carlos Ruiz",
            "c.ruiz@global.com",
            None,
        ),
    ]
    cursor.executemany(
        "INSERT INTO EMPRESAS (nombre, sector, direccion, ciudad, persona_contacto, correo_contacto, telefono_contacto) VALUES (%s, %s, %s, %s, %s, %s, %s);",
        empresas,
    )

    # Insertar ESTUDIANTES
    estudiantes = [
        (
            "12345678A",
            "Ana López",
            "2000-05-12",
            "ana.lopez@email.com",
            "600123456",
            "Española",
            1,
            "Ingeniería Informática",
            "2024/25",
        ),
        (
            "87654321B",
            "Pedro Martínez",
            "1999-11-03",
            "pedro.martinez@email.com",
            "600654321",
            "Española",
            2,
            "Administración de Empresas",
            "2024/25",
        ),
        (
            "11223344C",
            "Lucía García",
            "2001-02-20",
            "lucia.garcia@email.com",
            None,
            "Española",
            1,
            "Biología",
            "2024/25",
        ),
    ]
    cursor.executemany(
        "INSERT INTO ESTUDIANTES (dni, nombre, fecha_nacimiento, correo, telefono, nacionalidad, id_centro, titulacion, curso_academico) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);",
        estudiantes,
    )

    connection.commit()
    print("Se insertaron los datos de prueba correctamente.")

except mysql.connector.Error as err:
    print(f"Error al insertar datos de prueba: {err}")

# -----------------------------
# Consultar datos insertados
# -----------------------------
try:
    cursor.execute("SELECT * FROM CENTROS_EDUCATIVOS;")
    print("\nCentros Educativos:")
    for row in cursor.fetchall():
        print(row)

    cursor.execute("SELECT * FROM EMPRESAS;")
    print("\nEmpresas:")
    for row in cursor.fetchall():
        print(row)

    cursor.execute("SELECT * FROM ESTUDIANTES;")
    print("\nEstudiantes:")
    for row in cursor.fetchall():
        print(row)

except mysql.connector.Error as err:
    print(f"Error al consultar datos: {err}")

finally:
    if cursor:
        cursor.close()
    if connection:
        connection.close()
