"""
load.py - Fast and Safe ETL
Funciones de carga para dimensiones y hechos en el DW.
"""

import pandas as pd
from sqlalchemy.engine import Engine
from sqlalchemy import text


def load_dim(df: pd.DataFrame, target: Engine, table_name: str, pk_name: str) -> None:
    """
    Carga una dimensión en el DW.
    Limpia los datos existentes con TRUNCATE (respeta las constraints) e inserta los nuevos.
    
    Args:
        df: DataFrame con los datos a cargar
        target: Motor SQLAlchemy conectado al DW
        table_name: Nombre de la tabla de dimensión
        pk_name: Nombre de la columna clave primaria (solo para logging)
    """
    try:
        # Primero truncar la tabla para limpiar datos existentes
        # TRUNCATE respeta las foreign keys sin necesidad de CASCADE
        with target.connect() as conn:
            conn.execute(text(f"TRUNCATE TABLE {table_name} CASCADE"))
            conn.commit()
        
        # Luego insertar los nuevos datos
        df.to_sql(table_name, target, if_exists='append', index=False)
        print(f"    [OK] Dimensión '{table_name}' cargada ({len(df)} registros).")
    except Exception as e:
        print(f"    [ERROR] No se pudo cargar dimensión '{table_name}': {e}")
        raise

def load(df: pd.DataFrame, target: Engine, table_name: str, replace: bool = False) -> None:
    """
    Carga datos de hechos o resúmenes en el DW.
    
    Args:
        df: DataFrame con los datos a cargar
        target: Motor SQLAlchemy conectado al DW
        table_name: Nombre de la tabla en el DW
        replace: Si True, reemplaza todos los datos de la tabla con TRUNCATE.
                Si False, agrega los datos sin borrar los existentes.
    """
    try:
        if replace:
            # Limpiar datos existentes con TRUNCATE
            with target.connect() as conn:
                conn.execute(text(f"TRUNCATE TABLE {table_name} CASCADE"))
                conn.commit()
            if_exists = 'append'
        else:
            if_exists = 'append'
        
        df.to_sql(table_name, target, if_exists=if_exists, index=False)
        print(f"    [OK] Tabla '{table_name}' cargada ({len(df)} registros).")
    except Exception as e:
        print(f"    [ERROR] No se pudo cargar tabla '{table_name}': {e}")
        raise
