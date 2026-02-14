import pandas as pd
import psycopg2
from sqlalchemy import create_engine
from sqlalchemy.types import VARCHAR, NUMERIC, DATE, INTEGER  # ← ¡ESTAS 2 LÍNEAS FALTABAN!
import unicodedata

# 1. Leer CSV
df = pd.read_csv('../data/warranty_cases.csv')

print(df.shape)

print(df.head())

print(df.info())


#quitar filas duplicadas
df_warrantycase_limpio  = df.drop_duplicates(keep='last',inplace=False)


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

columnas = ['issue_description', 'approval_status']
df_warrantycase_limpio[columnas] = df_warrantycase_limpio[columnas].apply(quitar_acentos_mantener_ñ)
print("df_warrantycase_limpio: ")
print(df_warrantycase_limpio)
print(df_warrantycase_limpio.info())


#quitar espacios del medio y del final en string
def normalizar_espacios(col):
    return (col.astype(str)
            .str.replace(r'\s+', ' ', regex=True)  # Múltiples → 1 espacio
            .str.strip()                           # Quita espacios extremos
           )

columnas_texto = ['issue_description', 'approval_status']
df_warrantycase_limpio[columnas_texto] = df_warrantycase_limpio[columnas_texto].apply(normalizar_espacios)

print("df_warranycase_limpio: ")
print(df_warrantycase_limpio)
print(df_warrantycase_limpio.info())


# 7. Formatear  quitar decimales y convertir a entero
df_warrantycase_limpio['amount_approved'] = df_warrantycase_limpio['amount_approved'].round(2)
print('---------------------------------------------------------------')
print(df_warrantycase_limpio)
#df_final['consumo_promedio'] = df_final['consumo_promedio'].round(3)
#df_final['emisiones_co2_base'] = df_final['emisiones_co2_base'].round(2)

df_final = df_warrantycase_limpio

#castear a numerico las columnas de los montos amount approved y amount claimed
# De int → float con 2 decimales
df_final['amount_claimed'] = df_final['amount_claimed'].astype(float).round(2)


print("df_final a sql-ultimo: ")
print(df_final)
print(df_final.info())


engine = create_engine('postgresql://neondb_owner:npg_jhYlUONWJ6B7@ep-gentle-forest-aist7uh4-pooler.c-4.us-east-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require')
##engine = create_engine('postgresql://myuser:mypassword@localhost:5433/maquinaria_agroforestal')

#castear a numerico las columnas de los montos amount approved y amount claimed

# restriccion id en equipment debe existir para poder cargar en failures
df_equipment_dbwarrantycases = pd.read_sql("select equipment_id from equipment", engine)
ids_validosenequipment = df_equipment_dbwarrantycases['equipment_id'].unique()

print("df from equipment database:")
print(ids_validosenequipment)

df_final = df_final[df_final['equipment_id'].isin(ids_validosenequipment)].copy()

print("df_final a sql-ultimo: ")
print(df_final)
print(df_final.info())



# 9. CARGAR ✅ Todas las columnas de tu tabla
df_final.to_sql(
    name='warranty_cases',
    con=engine,
    if_exists='append',
    index=False,
    method=None,       
    dtype={
        'equipment_id': INTEGER,
        'customer_id': INTEGER,
        'case_open_date': DATE,
        'case_close_date': DATE,
        'issue_description': VARCHAR(100),
        'approval_status': VARCHAR(40),
        'amount_claimed': NUMERIC(10,2),
        'amount_approved': NUMERIC(10,2) # ← Incluida aquí
        
        
        
    }
)

print("✅ Datos cargados COMPLETOS en tabla warranty_cases!")


