import pandas as pd
import psycopg2
from sqlalchemy import create_engine
from sqlalchemy.types import VARCHAR, NUMERIC  # ← ¡ESTAS 2 LÍNEAS FALTABAN!

"""
# 1. Leer CSV
df = pd.read_csv('flota.csv')

# 2. Limpiar y mapear columnas para tu tabla
df_limpio = df[['ID_Flota', 'Tipo_Vehículo', 'Capacidad_Carga()', 'Consumo_Promedio(kWh/kmoL/100km)']].copy()

# 3. Renombrar columnas
df_limpio.columns = ['id_flota_raw', 'tipo_vehiculo', 'capacidad_carga', 'consumo_promedio']

# 4. Limpiar datos
df_limpio['id_flota_raw'] = df_limpio['id_flota_raw'].str.strip()  # Quitar espacios
df_limpio['tipo_vehiculo'] = df_limpio['tipo_vehiculo'].str.strip().str.title()  # Normalizar primera letra Mayuscula resto Minusculas
df_limpio['capacidad_carga'] = pd.to_numeric(df_limpio['capacidad_carga'], errors='coerce')
df_limpio['consumo_promedio'] = pd.to_numeric(df_limpio['consumo_promedio'], errors='coerce')

# 5. Eliminar filas con datos inválidos (NaN)
df_limpio = df_limpio.dropna()

# 6. Seleccionar SOLO columnas de tu tabla
df_final = df_limpio[['tipo_vehiculo', 'capacidad_carga', 'consumo_promedio']].copy()

# 7. Formatear para PostgreSQL
df_final['capacidad_carga'] = df_final['capacidad_carga'].round(2)
df_final['consumo_promedio'] = df_final['consumo_promedio'].round(3)
df_final = df_final.iloc[:-1] # quita ultima fila
print("Datos limpios:")
print(df_final)
print(df_final.head())
print(f"Filas válidas: {len(df_final)}")


# Tu DataFrame limpio (df_final del código anterior)
# Conexión PostgreSQL (ajusta credenciales)
engine = create_engine('postgresql://postgres:tu_password_seguro_123@localhost:5400/ecologistics_analitica')

# CARGAR (SERIAL genera id_flota automáticamente)
df_final.to_sql(
    name='flota',           # Nombre de tu tabla
    con=engine,             # Conexión
    if_exists='append',     # append = agregar, replace = reemplazar
    index=False,            # No incluir índice de pandas
    method='multi'          # Rápido para muchos datos
)

print("✅ Datos cargados en tabla flota!")
"""

# 1. Leer CSV
df = pd.read_csv('../data/customer.csv')

# 2. Limpiar y mapear SOLO columnas del CSV
df_limpio = df[['Tipo_Vehículo', 'Capacidad_Carga()', 'Consumo_Promedio(kWh/kmoL/100km)']].copy()

# 3. Renombrar columnas
df_limpio.columns = ['tipo_vehiculo', 'capacidad_carga', 'consumo_promedio']

# 4. Limpiar datos
df_limpio['tipo_vehiculo'] = df_limpio['tipo_vehiculo'].str.strip().str.title()
df_limpio['capacidad_carga'] = pd.to_numeric(df_limpio['capacidad_carga'], errors='coerce')
df_limpio['consumo_promedio'] = pd.to_numeric(df_limpio['consumo_promedio'], errors='coerce')

# 5. Eliminar filas inválidas
df_limpio = df_limpio.dropna()

# 6. Quitar última fila
df_final = df_limpio.iloc[:-1].copy()

# 🔥 AGREGAR COLUMNA emisiones_co2_base CON VALOR 0.00
df_final['emisiones_co2_base'] = 0.00 

"""
dataf = pd.read_csv('mi-csv'): importamos el csv con los datos a python y cargamos el dataframe

dataf.shape: saber la cantidad de filas y columnas del dataset: ej (2845,9) 2845 filas por 9 columnas

dataf.head(): muestra las primeras 5 filas y todas las columnas del dataset

dataf.info(): muestra las diferentes columnas que tenemos en el dataset las diferentes filas y la cantidad de valores no nulos en estas columnas
 y el tipo de dato de esa columna en particular

 dataf.drop_duplicates(): evalua todo el dataset y elimina las filas duplicadas
dentro de esta funcion podemos definir las columnas del dataset que queremos evaluar entonces podemos eliminar filas que tengan duplicados en determinadas columnas

dataf_sin_nulos = dataf.dropna(): elimina filas que contienen al menos un valor nulo

dataf_sn_nulos_columnas = dataf.dropna(axis=1): elimina columnas que contienen al menos un valor nulo

la segunda opcion es reemplazar estos valores nulos por otro valor (transformar)
puede ser cualquier otro valor o gralmente por el promedio de la columna en cuestion

si calculo la media sobre un campo no numerico obtengo un error, solo podemos hacer opreaciones
matematicas con valores numericos 

que hacemos ?
toma una columna salary que contiene valores en formato de cadena, se elimina cualquier caracter no numerico
extrae los digitos y convierte la columna resultante en valores numericos de tipo float
el resultado se almacena en la nueva columna salary_numeric
dataf_sin_nulos['salary_numeric'] = dataf_sin_nulos['salary'].replace(['^\d.'], '', regex=True).str.extract('(\d+)').astype(float)

dataf_sin_nulos= dataf_sin_nulos['salary_numeric'].describe(): nos brinda una estadistica descriptiva promedio percentiles de la columna numerica en cuestion
 cuando tenermos un datframe sucio , es decir por ejemplo que algunos puestos nos indiquen en la columna salario
 el salario anual y en otros puestos el salario por hora vemos una ditribucion desbalanceada
 de valores en el (histograma)
 quiero filtrar el dataset y quedarme con filas que tengan un salario > 20000
 como hago esto ?
 dataf_sin_nulos = dataf_sin_nulos[dataf_sin_nulos['salary_numeric'] >= 20000]

 esto dice que solo me quiero quedar con valores numericos >= 20000
  en relacion a la columna que parseamos anteriormente 'salary_numeric'
rellenar valores nulos con un valor especifico porej: promedio o media
  dataf_rellenado_con_promedio = dataf_sin_nulos.fillna(df_sin_nulos['salary_numeric'].mean())

  eliminar columnas que no necesitamos:
  dataf_rellenado_con_promedio.drop('salary', axis=1, inplace=True)

  agrupar y agregar datos

  supongamos que tenes un dataframe df con columnas 'title' y 'salary'
  agrupar por titulo y colocar el promedio y la mediana para cada titulo
  df_grouped = df_con_relleno.groupby('title')['salary_numeric'].agg(['mean', 'median', 'count']).reset_index()
  renombrar las columnas para mayor claridad
    df_grouped.columns = ['title', 'salary_mean', 'salary_median', 'job_count']
    si llegase a quedar .00000 que son todos los decimales y queda largo el numero se redondea con:
    df_grouped['salary_median'] = df_grouped['salary_median'].round(0).astype(int)
    lo redondea y castea a 0


"""

# 7. Formatear PRECISAMENTE como tu tabla
df_final['capacidad_carga'] = df_final['capacidad_carga'].round(2)
df_final['consumo_promedio'] = df_final['consumo_promedio'].round(3)
df_final['emisiones_co2_base'] = df_final['emisiones_co2_base'].round(2)

print("Datos limpios (CON emisiones_co2_base):")
print(df_final)
print("--------------------------------------------------")
print(df_final.head())
print(f"Filas válidas: {len(df_final)}")
print("Columnas:", df_final.columns.tolist())  # Verifica las 4 columnas

# 8. Conexión PostgreSQL
engine = create_engine('postgresql://postgres:tu_password_seguro_123@localhost:5400/ecologistics_analitica')

# 9. CARGAR ✅ Todas las 4 columnas de tu tabla
df_final.to_sql(
    name='flota',
    con=engine,
    if_exists='append',
    index=False,
    method='multi',
    dtype={
        'tipo_vehiculo': VARCHAR(50),
        'capacidad_carga': NUMERIC(10,2),
        'consumo_promedio': NUMERIC(6,3),
        'emisiones_co2_base': NUMERIC(8,2)  # ← Incluida aquí
    }
)

print("✅ Datos cargados COMPLETOS en tabla flota!")
print("✅ id_flota (SERIAL) y emisiones_co2_base (0.00) generados automáticamente")


