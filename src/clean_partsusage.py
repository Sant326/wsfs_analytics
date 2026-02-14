import pandas as pd
import psycopg2
from sqlalchemy import create_engine
from sqlalchemy.types import VARCHAR, NUMERIC, DATE, INTEGER  # ← ¡ESTAS 2 LÍNEAS FALTABAN!
import unicodedata

# 1. Leer CSV
df = pd.read_csv('../data/parts_usage.csv')

print(df.shape)

print(df.head())

print(df.info())




# 7. Formatear  quitar decimales y convertir a entero
df_partsusage_limpio = pd.DataFrame()
df_partsusage_limpio = df.drop_duplicates(keep='last',inplace=False)
# Elimina filas donde quantity es NaN/None
df_partsusage_limpio = df.dropna(axis=0,
    subset=['quantity', "warehouse_location"],    # Solo chequea quantity
    how='any'              # Si hay NaN (default)
).copy()




print("df_partsusage_limpio: ")
print(df_partsusage_limpio)
print(df_partsusage_limpio.info())

df_partsusage_limpio['quantity'] = df_partsusage_limpio['quantity'].round(0).astype(int)


print("df_partsusage_limpio arreglado quantity: ")
print(df_partsusage_limpio)
print(df_partsusage_limpio.info())

df_partsusage_limpio['unit_price'] = df_partsusage_limpio['unit_price'].round(2)
df_partsusage_limpio['total_price'] = df_partsusage_limpio['total_price'].round(2)

print("df_partsusage_limpio reodndeado unit y total price: ")
print(df_partsusage_limpio)
print(df_partsusage_limpio.info())


#quitar espacios del medio y del final en string
def normalizar_espacios(col):
    return (col.astype(str)
            .str.replace(r'\s+', ' ', regex=True)  # Múltiples → 1 espacio
            .str.strip()                           # Quita espacios extremos
           )

columnas_texto = ['warehouse_location']
df_partsusage_limpio[columnas_texto] = df_partsusage_limpio[columnas_texto].apply(normalizar_espacios)

df_final = df_partsusage_limpio


print("df_final a sql-ultimo: ")
print(df_final)
print(df_final.info())

engine = create_engine('postgresql://neondb_owner:npg_jhYlUONWJ6B7@ep-gentle-forest-aist7uh4-pooler.c-4.us-east-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require')
##engine = create_engine('postgresql://myuser:mypassword@localhost:5433/maquinaria_agroforestal')



# restriccion id en parts debe existir para poder cargar en parts_usage
df_parts_dbpartsusage = pd.read_sql("select part_id from parts", engine)
ids_validosparts = df_parts_dbpartsusage['part_id'].unique()

print("df from parts database:")
print(ids_validosparts)

df_final = df_final[df_final['part_id'].isin(ids_validosparts)].copy()

print("df_final a sql-ultimo: ")
print(df_final)
print(df_final.info())




# restriccion id en service_order debe existir para poder cargar en parts_sales
df_serviceorder_dbpartsusage = pd.read_sql("select service_id from service_order", engine)
ids_validosenserviceorder = df_serviceorder_dbpartsusage['service_id'].unique()

print("df from serviceorder database:")
print(ids_validosenserviceorder)

df_final = df_final[df_final['service_id'].isin(ids_validosenserviceorder)].copy()

print("df_final a sql-ultimo: ")
print(df_final)
print(df_final.info())


# 9. CARGAR ✅ Todas las columnas de tu tabla
df_final.to_sql(
    name='parts_usage',
    con=engine,
    if_exists='append',
    index=False,
    method=None,
    dtype={
        'service_id': INTEGER,
        'part_id': INTEGER,
        'quantity': INTEGER,
        'unit_price': NUMERIC(10,2),
        'total_price': NUMERIC(10,2),
        'warehouse_location': VARCHAR(10)
     
        
    }
)

print("✅ Datos cargados COMPLETOS en tabla sales_usage!")



#print("df_partsusage_limpio: ")
#print(df_partsusage_limpio)
#print(df_partsusage_limpio.info())

#df_partsusage_limpio = df_partsusage_limpio.drop_duplicates(keep='last',inplace=False)

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
def noralizar_espacios(col):
    return (col.astype(str)
            .str.replace(r'\s+', ' ', regex=True)  # Múltiples → 1 espacio
            .str.strip()                           # Quita espacios extremos
           )

columnas_texto = ['sale_type']
df_partssales_limpio[columnas_texto] = df_partssales_limpio[columnas_texto].apply(normalizar_espacios)

df_final = df_partssales_limpiom


print("df_final a sql-ultimo: ")
print(df_final)
print(df_final.info())


#columns sale_line_id,sale_date,customer_id,part_id,quantity,unit_price,total_price,sale_type,service_id,invoice_number
engine = create_engine('postgresql://postgres:tu_password_seguro_123@localhost:5400/maquinaria_agroforestal')


# leemos nuestra base de datos
df_customer_dbpartsales = pd.read_sql("select customer_id from customer", engine)
#obtenemos customer_id
ids_validos = df_customer_dbpartsales['customer_id'].unique()

print("df from customer database:")
print(ids_validos)


df_final = df_final[df_final['customer_id'].isin(ids_validos)].copy()


# restriccion id en parts debe existir para poder cargar en parts_sales
df_parts_dbpartsales = pd.read_sql("select part_id from parts", engine)
ids_validosparts = df_parts_dbpartsales['part_id'].unique()

print("df from parts database:")
print(ids_validosparts)

df_final = df_final[df_final['part_id'].isin(ids_validosparts)].copy()

print("df_final a sql-ultimo: ")
print(df_final)
print(df_final.info())




# restriccion id en service_order debe existir para poder cargar en parts_sales
df_serviceorder_dbpartsales = pd.read_sql("select service_id from service_order", engine)
ids_validosenserviceorder = df_serviceorder_dbpartsales['service_id'].unique()

print("df from serviceorder database:")
print(ids_validosenserviceorder)

df_final = df_final[df_final['service_id'].isin(ids_validosenserviceorder)].copy()

print("df_final a sql-ultimo: ")
print(df_final)
print(df_final.info())



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

"""


