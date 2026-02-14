import pandas as pd
import psycopg2
from sqlalchemy import create_engine
from sqlalchemy.types import VARCHAR, NUMERIC, DATE, INTEGER  # ← ¡ESTAS 2 LÍNEAS FALTABAN!
import unicodedata

# 1. Leer CSV
df = pd.read_csv('../data/service_order.csv')

print(df.shape)

print(df.head())

print(df.info())


#quitar filas duplicadas
df_serviceorder_limpio  = df.drop_duplicates(keep='last',inplace=False)


#quitar acentos , mantener ñ y quitar espacios
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

columnas = ['reported_issue','root_cause', 'technician', 'status']
df_serviceorder_limpio[columnas] = df[columnas].apply(quitar_acentos_mantener_ñ)
print("df_serviceorder_limpio: ")
print(df_serviceorder_limpio)
print(df_serviceorder_limpio.info())



#quitar espacios del medio y del final en string
def normalizar_espacios(col):
    return (col.astype(str)
            .str.replace(r'\s+', ' ', regex=True)  # Múltiples → 1 espacio
            .str.strip()                           # Quita espacios extremos
           )

columnas_texto = ['reported_issue','root_cause', 'technician', 'status']
df_serviceorder_limpio[columnas_texto] = df_serviceorder_limpio[columnas_texto].apply(normalizar_espacios)

print("df_serviceorder_limpio: ")
print(df_serviceorder_limpio)
print(df_serviceorder_limpio.info())

df_final = df_serviceorder_limpio


print("df_final a sql-ultimo: ")
print(df_final)
print(df_final.info())



#columns: service_id,equipment_id,customer_id,service_type,start_date,end_date,reported_issue,root_cause,technician,status
engine = create_engine('postgresql://neondb_owner:npg_jhYlUONWJ6B7@ep-gentle-forest-aist7uh4-pooler.c-4.us-east-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require')
##engine = create_engine('postgresql://myuser:mypassword@localhost:5433/maquinaria_agroforestal')

# leemos nuestra base de datos
df_equipment_db = pd.read_sql("select equipment_id from equipment", engine)
#obtenemos customer_id
ids_validos = df_equipment_db['equipment_id'].unique()

print("df from equipment database:")
print(ids_validos)


df_serviceorder_limpio = df_serviceorder_limpio[df_serviceorder_limpio['equipment_id'].isin(ids_validos)].copy()


df_final = df_serviceorder_limpio

print("df_final a sql-ultimo: ")
print(df_final)
print(df_final.info())


# 9. CARGAR ✅ Todas las columnas de tu tabla
df_final.to_sql(
    name='service_order',
    con=engine,
    if_exists='append',
    index=False,
    method=None,       
    dtype={
        'equipment_id': INTEGER,
        'customer_id': INTEGER,
        'service_type': INTEGER,
        'start_date': DATE,
        'end_date': DATE,
        'reported_issue': VARCHAR(50),
        'root_cause': VARCHAR(50),
        'technician': VARCHAR(40),  # ← Incluida aquí
        'status': VARCHAR(40)
        
        
    }
)

print("✅ Datos cargados COMPLETOS en tabla service_order!")



"""


#quitar filas duplicadas en todo el dataset
df_partssales_limpio = df.drop_duplicates(keep='last',inplace=False)
# imprime la etiqueta y hace un salto de linea y muestra el dataframe
print(f"df-partssales-limpio\n{df_partssales_limpio}")
print(df_partssales_limpio.info())

# rellenar con cero columnas n/a de service_id
df_partssales_limpio['service_id'] = df_partssales_limpio['service_id'].fillna(0)


# 7. Formatear  quitar decimales y convertir a entero
df_partssales_limpio['service_id'] = df_partssales_limpio['service_id'].round(0).astype(int)
print('---------------------------------------------------------------')
print(df_partssales_limpio)
#df_final['consumo_promedio'] = df_final['consumo_promedio'].round(3)
#df_final['emisiones_co2_base'] = df_final['emisiones_co2_base'].round(2)

#df_final['capacidad_carga'] = df_final['capacidad_carga'].round(2)
columnas = ['unit_price', 'total_price']
df_partssales_limpio[columnas] = df_partssales_limpio[columnas].round(2)
print('df_parts_sales_limpio: ')
print('---------------------------------------------------------------')
print(df_partssales_limpio)
print(df_partssales_limpio.info())
#chequear > 0 columnas precio
df_partssales_limpio = df_partssales_limpio.query('unit_price > 0 and total_price > 0')
print('df_parts_sales_limpio: price > 0: ')
print('---------------------------------------------------------------')
print(df_partssales_limpio)
print(df_partssales_limpio.info())


#quitar acentos , mantener ñ y quitar espacios
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

columnas = ['sale_type']
df_partssales_limpio[columnas] = df_partssales_limpio[columnas].apply(quitar_acentos_mantener_ñ)
print("df_final: ")
print(df_partssales_limpio)



#quitar espacios del medio y del final en string
def normalizar_espacios(col):
    return (col.astype(str)
            .str.replace(r'\s+', ' ', regex=True)  # Múltiples → 1 espacio
            .str.strip()                           # Quita espacios extremos
           )

columnas_texto = ['sale_type']
df_partssales_limpio[columnas_texto] = df_partssales_limpio[columnas_texto].apply(normalizar_espacios)

df_final = df_partssales_limpio


print("df_final a sql-ultimo: ")
print(df_final)
print(df_final.info())

#columns sale_line_id,sale_date,customer_id,part_id,quantity,unit_price,total_price,sale_type,service_id,invoice_number
engine = create_engine('postgresql://postgres:tu_password_seguro_123@localhost:5400/maquinaria_agroforestal')



# 9. CARGAR ✅ Todas las columnas de tu tabla
df_final.to_sql(
    name='parts_sales',
    con=engine,
    if_exists='append',
    index=False,
    method=None,       
    dtype={
        'sale_date': DATE,
        'customer_id': INTEGER,
        'part_id': INTEGER,
        'quantity': INTEGER,
        'unit_price': NUMERIC(10,2),
        'total_price': NUMERIC(10,2),
        'sale_type': VARCHAR(20),
        'service_id': INTEGER,  # ← Incluida aquí
        'invoice_number': VARCHAR(30)
        
        
    }
)

print("✅ Datos cargados COMPLETOS en tabla parts_sales!")





#df_sales_limpio = df_sales_limpio.dropna(subset=['salesperson'], how='all') 

# rellenar espacios nulos con el string cajero en la columna salesperson
df_sales_limpio['salesperson'] = df_sales_limpio['salesperson'].fillna('cajero') 
print('-------------------------------------------------------------')
print(f"df-sales-limpio\n{df_sales_limpio}")
print(df_sales_limpio.info())

#quitar espacios del medio y del final en stringd
def normalizar_espacios(col):
    return (col.astype(str)
            .str.replace(r'\s+', ' ', regex=True)  # Múltiples → 1 espacio
            .str.strip()                           # Quita espacios extremos
           )

columnas_texto = ['payment_terms', 'salesperson', 'model']
df_sales_limpio[columnas_texto] = df_sales_limpio[columnas_texto].apply(normalizar_espacios)

#quitar acentos , mantener ñ y quitar espacios
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

columnas = ['payment_terms', 'salesperson', 'model']
df_sales_limpio[columnas] = df_sales_limpio[columnas].apply(quitar_acentos_mantener_ñ)
print("df_final: ")
print(df_sales_limpio)

df_final = df_sales_limpio

print("df_final a sql-ultimo: ")
print(df_final)
print(df_final.info())

#columns:sale_id,customer_id,model,serial_number,year,sale_date,sale_price,warranty_months,payment_terms,delivery_date,salesperson

engine = create_engine('postgresql://postgres:tu_password_seguro_123@localhost:5400/Maquinaria_AgroForestal')



# 9. CARGAR ✅ Todas las columnas de tu tabla
df_final.to_sql(
    name='sales',
    con=engine,
    if_exists='append',
    index=False,
    method='multi',
    dtype={
        'customer_id': INTEGER,
        'model': VARCHAR(50),
        'serial_number': VARCHAR(100),
        'year': INTEGER,
        'sale_date': DATE,
        'sale_price': INTEGER,
        'warranty_months': NUMERIC(10,2),
        'payment_terms': VARCHAR(50),  # ← Incluida aquí
        'delivery_date': DATE,
        'salesperson': VARCHAR(100)
        
    }
)

print("✅ Datos cargados COMPLETOS en tabla sales!")





# columnas: equipment_id,serial_number,model,year,customer_id,current_status, hours_worked, location_department, location_city
df_equipment_limpio = df.drop_duplicates(keep='last',inplace=False)
# imprime la etiqueta y hace un salto de linea y muestra el dataframe
print(f"df limpio\n{df_equipment_limpio}")
print(df_equipment_limpio.info())

def limpiar_string(col):
    return col.astype(str).str.strip().str.lower()
# TRANASFORMAR A MINUSCULA Y QUITAR ESPACIOS
columnas_limpias = ['model', 'current_status', 'location_department', 'location_city']
df_equipment_limpio[columnas_limpias] = df_equipment_limpio[columnas_limpias].apply(limpiar_string)

print(f"df_equipment_limpio\n{df_equipment_limpio}")
print(df_equipment_limpio.info())


df_equipment_limpio = df_equipment_limpio.query(
    'year > 0 and hours_worked > 0'
)

print(f"df_equipment_limpio_filtrado\n{df_equipment_limpio}")
print(df_equipment_limpio.info())

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

columnas = ['location_department', 'location_city', 'current_status', 'model']
df_equipment_limpio[columnas] = df_equipment_limpio[columnas].apply(quitar_acentos_mantener_ñ)
print("df_final: ")
print(df_equipment_limpio)

df_final = df_equipment_limpio

print("df_final a sql: ")
print(df_final)
print(df_final.info())
#quitar espacios del medio y del final en stringd
def normalizar_espacios(col):
    return (col.astype(str)
            .str.replace(r'\s+', ' ', regex=True)  # Múltiples → 1 espacio
            .str.strip()                           # Quita espacios extremos
           )

columnas_texto = ['model', 'current_status', 'location_city', 'location_department']
df_final[columnas_texto] = df_final[columnas_texto].apply(normalizar_espacios)


print("df_final a sql-ultimo: ")
print(df_final)
print(df_final.info())


engine = create_engine('postgresql://postgres:tu_password_seguro_123@localhost:5400/Maquinaria_AgroForestal')
# columnas en csv customer_id, customer_number, customer_name, creation_date, department, city, address, phone, email, status
# columnas: equipment_id,serial_number,model,year,customer_id,current_status, hours_worked, location_department, location_city



# 9. CARGAR ✅ Todas las columnas de tu tabla
df_final.to_sql(
    name='equipment',
    con=engine,
    if_exists='append',
    index=False,
    method='multi',
    dtype={
        'serial_number': VARCHAR(50),
        'model': VARCHAR(50),
        'year': INTEGER,
        'customer_id': INTEGER,
        'current_status': VARCHAR(50),
        'hours_worked': INTEGER,
        'location_department': VARCHAR(50),
        'location_city': VARCHAR(50)  # ← Incluida aquí
        
    }
)

print("✅ Datos cargados COMPLETOS en tabla equipment!")





para todas las columnas texto
columnas_string = df.select_dtypes(include=['object']).columns
df[columnas_string] = df[columnas_string].apply(normalizar_espacios)
df_customer_limpio = df_customer_limpio.dropna(subset=['phone', 'email'], how='all')  # Solo si ambas nulas
print("df_customer_limpio informacion")
print(df_customer_limpio.info())
print(df_customer_limpio)

df_customer_limpio['email'] = df_customer_limpio['email'].fillna('no_email@ejemplo.com') 
print("df_sin_email_nulo")
print(df_customer_limpio.info())
print(df_customer_limpio)


df_customer_limpio['phone'] = df_customer_limpio['phone'].fillna('+8888888888') 
print("df_sin_phone_nulo")
print(df_customer_limpio.info())
print(df_customer_limpio)

"""