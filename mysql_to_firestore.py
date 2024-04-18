import pymysql
import firebase_admin
from firebase_admin import credentials, firestore
import datetime
from pytz import utc

# Inicializar Firebase Admin
cred = credentials.Certificate('serviceAccountKey.json')
firebase_admin.initialize_app(cred)
db = firestore.client()

# Configuración de conexión a MySQL
mysql_config = {
    'host': '...',
    'user': '...',
    'password': '...'
}

# Conectar a MySQL
connection = pymysql.connect(**mysql_config)

# Función para convertir datos a formatos compatibles con Firestore
def convert_to_firestore_format(value):
    if isinstance(value, datetime.date) and not isinstance(value, datetime.datetime):
        # Convierte date a datetime con hora mínima y zona horaria UTC
        return datetime.datetime(value.year, value.month, value.day, tzinfo=utc)
    return value

try:
    with connection.cursor() as cursor:
        # Obtener la lista de bases de datos con el prefijo "db_"
        cursor.execute("SHOW DATABASES")
        databases = [db for db, in cursor.fetchall() if db.startswith("db_")]

        for database in databases:
            print(f"Procesando la base de datos: {database}")
            cursor.execute(f"USE {database};")
            # Crear una colección en Firestore para cada base de datos
            db_collection = db.collection(database)

            # Define las tablas que serán migradas
            tables = [
                "tabla1", "tabla2", "tabla3"
            ]

            for table in tables:
                print(f"    Migrando tabla: {table}")
                cursor.execute(f"SELECT * FROM {table}")
                rows = cursor.fetchall()
                fields = [i[0] for i in cursor.description]
                # Crea un documento para cada tabla
                table_doc = db_collection.document(table)
                # Cada registro se convierte en un documento dentro de una subcolección llamada 'records'
                records_collection = table_doc.collection('records')

                for row in rows:
                    # Convertir cada registro en un diccionario y ajustar los tipos de datos para Firestore
                    record = {fields[i]: convert_to_firestore_format(row[i]) for i in range(len(fields))}
                    # Añadir cada registro como un nuevo documento en Firestore
                    records_collection.add(record)

finally:
    connection.close()
    print("Migración completada.")