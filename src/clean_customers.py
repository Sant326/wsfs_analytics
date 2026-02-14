import pandas as pd
import psycopg2
from sqlalchemy import create_engine
from sqlalchemy.types import VARCHAR, NUMERIC, DATE  # ← ¡ESTAS 2 LÍNEAS FALTABAN!
import unicodedata

# 1. Leer CSV
df = pd.read_csv('../data/customer.csv')

print(df.shape)

print(df.head())

print(df.info())

df_customer_limpio = df.drop_duplicates(keep='last',inplace=False)
# imprime la etiqueta y hace un salto de linea y muestra el dataframe
print(f"df limpio\n{df_customer_limpio}")
print(df.info())

df_customer_limpio = df_customer_limpio.dropna(subset=['phone', 'email'], how='all')  # Solo si ambas nulas
print("df_customer_limpio informacion")
print(df_customer_limpio.info())
print(df_customer_limpio)
# volver a nullo porque debe ser unico el mail
#df_customer_limpio['email'] = df_customer_limpio['email'].fillna('no_email@ejemplo.com')
df_customer_limpio['email'] = df_customer_limpio['email'].replace('no_email@ejemplo.com', pd.NA) 
print("df_con_email_nulo")
print(df_customer_limpio.info())
print(df_customer_limpio)




df_customer_limpio['phone'] = df_customer_limpio['phone'].fillna('+8888888888') 
print("df_sin_phone_nulo")
print(df_customer_limpio.info())
print(df_customer_limpio)



df_customer_limpio['address'] = df_customer_limpio['address'].fillna('no-address') 
print("df_sin_address_nulo\n")
print(df_customer_limpio.info())
print(df_customer_limpio)

def limpiar_string(col):
    return col.astype(str).str.strip().str.lower()
# TRANASFORMAR A MINUSCULA Y QUITAR ESPACIOS
columnas_limpias = ['customer_name', 'department', 'city', 'address', 'phone', 'email', 'status']
df_customer_limpio[columnas_limpias] = df_customer_limpio[columnas_limpias].apply(limpiar_string)


"""
# QUITAR CARACTERES ESPECIALES no se usa para este caso
columnas = ['customer_name']
df_customer_limpio[columnas] = df_customer_limpio[columnas].apply(lambda x: x.str.replace(r'[^a-zA-Z0-9@.\s-]', '', regex=True)
                                  .str.strip().str.lower())

print("df_normalizado_sin caracteres especiales\n")
print(df_customer_limpio.info())
print(df_customer_limpio)
"""
def quitar_acentos_mantener_ñ(col):
    # Trabaja sobre strings
    col = col.astype(str)

    # Normaliza (descompone caracteres con acento)
    col = col.apply(lambda x: unicodedata.normalize('NFKD', x))

    # Filtra solo las letras base (sin el signo de acento), pero deja ñ tal cual
    col = col.apply(lambda x: ''.join(
        c for c in x
        if not unicodedata.combining(c)  # quita marcas de acento
    ))

    # Limpieza extra (opcional)
    col = col.str.strip().str.lower()
    return col

columnas = ['customer_name']
df_customer_limpio[columnas] = df_customer_limpio[columnas].apply(quitar_acentos_mantener_ñ)
print(df_customer_limpio)

df_final = df_customer_limpio
print("df_final:")
print(df_final)
print(df_final.info())

df_final = df_final.drop_duplicates(subset=['email'], keep='last')


print("df_final sin duplicados de email:")
print(df_final)
print(df_final.info())
#df_customer_limpio = df_customer_limpio.drop_duplicates()

##engine = create_engine('postgresql://myuser:mypassword@localhost:5433/maquinaria_agroforestal') -- funciona para el postgres de docker
engine = create_engine('postgresql://neondb_owner:npg_jhYlUONWJ6B7@ep-gentle-forest-aist7uh4-pooler.c-4.us-east-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require')
# columnas en csv customer_id, customer_number, customer_name, creation_date, department, city, address, phone, email, status

# 9. CARGAR ✅ Todas las columnas de tu tabla
df_final.to_sql(
    name='customer',
    con=engine,
    if_exists='append',
    index=False,
    method='multi',
    dtype={
        'customer_number': VARCHAR(40),
        'customer_name': VARCHAR(50),
        'creation_date': DATE,
        'address': VARCHAR(50),
        'phone': VARCHAR(50),
        'email': VARCHAR(50),
        'status': VARCHAR(20),
        'department': VARCHAR(50),  # ← Incluida aquí
        'city': VARCHAR(50)
    }
)

print("✅ Datos cargados COMPLETOS en tabla customer!")










