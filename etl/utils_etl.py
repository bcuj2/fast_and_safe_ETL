"""
utils_etl.py - Fast and Safe ETL
Funciones auxiliares: verificación de conexión, inicialización del esquema DW.
"""

import yaml
import psycopg2
from sqlalchemy import inspect, text
from sqlalchemy.engine import Engine


def init_schema(target: Engine, sql_path: str = 'sqlscripts.yml'):
    """
    Crea las tablas del DW si no existen todavía.
    Lee los scripts SQL desde sqlscripts.yml en el orden correcto
    (primero dimensiones, luego hechos).
    """
    with open(sql_path, 'r', encoding='utf-8') as f:
        scripts = yaml.safe_load(f)

    # Orden de creación: primero dims, luego hechos
    orden = [
        'dim_fecha', 'dim_cliente', 'dim_sede', 'dim_mensajero',
        'dim_ciudad', 'dim_tipo_servicio', 'dim_estado_servicio',
        'dim_tipo_novedad', 'dim_hora', 'hecho_servicio', 'hecho_transiciones_estado',
        'resumen_novedades',
    ]

    with target.connect() as conn:
        for key in orden:
            if key in scripts:
                conn.execute(text(scripts[key]))
                conn.commit()
                print(f"  [SCHEMA] Tabla '{key}' verificada/creada.")


def test_connection(engine: Engine, name: str = ''):
    """Verifica que la conexión a la BD esté activa."""
    try:
        with engine.connect() as conn:
            conn.execute(text('SELECT 1'))
        print(f"  [OK] Conexión '{name}' establecida.")
        return True
    except Exception as e:
        print(f"  [ERROR] No se pudo conectar a '{name}': {e}")
        return False


def schema_exists(target: Engine) -> bool:
    """Retorna True si el esquema DW ya fue inicializado (existe al menos dim_fecha)."""
    inspector = inspect(target)
    return 'dim_fecha' in inspector.get_table_names()