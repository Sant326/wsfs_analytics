import pandas as pd
from sqlalchemy import create_engine, text
from pathlib import Path
from dotenv import load_dotenv
from datetime import date
import os

# Carga .env
BASE_DIR = Path(__file__).resolve().parent  # directorio raiz dentro de wsfs_analytics
ENV_PATH = BASE_DIR / "docker" / ".env"
load_dotenv(ENV_PATH)

# O Opción Avanzada (selecciona según ambiente)
ENVIRONMENT = os.getenv('ENVIRONMENT', 'production')

def get_db_connection():
    if ENVIRONMENT == 'production':
        connection_string = os.getenv('DATABASE_URL')
    else:
        """Conexión segura a PostgreSQL"""
        connection_string = (
            f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}"
            f"@{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"
        )
    return create_engine(connection_string)


def load_sales():
    """Carga tabla parts_sales"""
    engine = get_db_connection()
    query = """
    SELECT 
        salesperson as vendedor,  
        SUM(sale_price) AS monto_vendido
    FROM sales 
    GROUP BY salesperson
	HAVING SUM(sale_price) > 0 
    ORDER BY SUM(sale_price) DESC 
    LIMIT 3
    """
    return pd.read_sql(query, engine)


def load_customers():
    """Carga customers"""
    engine = get_db_connection()
    return pd.read_sql(
        "SELECT customer_id, email, name FROM customers LIMIT 100", engine
    )


def load_payment_terms(fecha_inicio, fecha_fin):
    engine = get_db_connection()
    query = text("""
    select payment_terms as FORMA_PAGO, count(*) AS CANTIDAD_VENTAS from sales 
    WHERE sale_date BETWEEN :fecha_inicio AND :fecha_fin
    group by payment_terms 
    ORDER BY CANTIDAD_VENTAS DESC
    """)
    if isinstance(fecha_inicio, str):
        fecha_inicio = date.fromisoformat(fecha_inicio.split("T")[0])
    if isinstance(fecha_fin, str):
        fecha_fin = date.fromisoformat(fecha_fin.split("T")[0])

    return pd.read_sql(
        query, engine, params={"fecha_inicio": fecha_inicio, "fecha_fin": fecha_fin}
    )


def load_failures_by_city():
    engine = get_db_connection()
    query = """
    SELECT 
        location_city as ciudad, 
        COUNT(f.failure_id) as total_fallas
    FROM equipment eq
    INNER JOIN failures f ON eq.equipment_id = f.equipment_id
    GROUP BY ciudad
    HAVING COUNT(f.failure_id) > 0
    ORDER BY COUNT(f.failure_id) DESC
    """
    return pd.read_sql_query(query, engine)  # Tu conexión PostgreSQL


def load_ventas_detfechas(fecha_inicio, fecha_fin):
    """
    Consulta parametrizada con SQLAlchemy - ¡SEGURA contra SQL Injection!
    """
    # Tu conexión (ajusta la URL)
    engine = create_engine("postgresql://usuario:password@localhost/tu_db")

    query = text("""
        SELECT 
            parts.description,
            COUNT(*) as cantidad_ventas
        FROM parts 
        INNER JOIN parts_sales ON parts.part_id = parts_sales.part_id
        WHERE sale_date BETWEEN :fecha_inicio AND :fecha_fin
        GROUP BY parts.description
        ORDER BY cantidad_ventas DESC
    """)

    # 🔥 PARAMETROS SEGUROS (NO concatenación de strings)
    df = pd.read_sql_query(
        query, engine, params={"fecha_inicio": fecha_inicio, "fecha_fin": fecha_fin}
    )

    engine.dispose()
    return df


def load_failures_by_city_wparams(total_falla):
    engine = get_db_connection()
    query = text("""
      SELECT 
        location_city as ciudad, 
        COUNT(f.failure_id) as total_fallas
    FROM equipment eq
    INNER JOIN failures f ON eq.equipment_id = f.equipment_id
    GROUP BY ciudad
    HAVING COUNT(f.failure_id) > 0 AND (:total_falla IS NULL OR COUNT(f.failure_id) = :total_falla)
    ORDER BY total_fallas DESC
    """)
    df = pd.read_sql_query(query, engine, params={"total_falla": total_falla})
    engine.dispose()
    return df


def montoventas_xtiponegocio(fecha):
    engine = get_db_connection()
    query = text("""    
    WITH ventas_consolidadas AS (
        SELECT 'Maquinaria' AS tipo_negocio, sale_price AS monto, sale_date AS fecha
        FROM sales
        UNION ALL
        SELECT 'Repuestos' AS tipo_negocio, total_price AS monto, sale_date AS fecha
        FROM parts_sales
    )
    SELECT 
    tipo_negocio, 
    SUM(monto) AS total_ingresos
    FROM ventas_consolidadas
    WHERE fecha >= :fecha -- Ejemplo de filtro desde una fecha específica
    GROUP BY tipo_negocio;
    """)
    df = pd.read_sql_query(query, engine, params={"fecha": fecha})
    engine.dispose()
    return df
