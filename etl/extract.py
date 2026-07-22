"""
extract.py - Fast and Safe ETL
Extrae datos de la BD operacional (fuente) y del DW (para construir hechos)
"""

import pandas as pd
from sqlalchemy.engine import Engine


# ─────────────────────────────────────────────
#  EXTRACCIÓN DE DIMENSIONES (desde BD fuente)
# ─────────────────────────────────────────────

def extract_cliente(source: Engine) -> pd.DataFrame:
    """Extrae clientes con su sector económico."""
    return pd.read_sql_query("""
        SELECT c.cliente_id, c.nit_cliente, c.nombre, c.sector
        FROM cliente c
    """, source)


def extract_sede(source: Engine) -> pd.DataFrame:
    """Extrae sedes con su ciudad y cliente asociado."""
    return pd.read_sql_query("""
        SELECT s.sede_id, s.nombre, s.direccion, s.cliente_id,
               ci.nombre AS nombre_ciudad
        FROM sede s
        JOIN ciudad ci ON ci.ciudad_id = s.ciudad_id
    """, source)


def extract_mensajero(source: Engine) -> pd.DataFrame:
    """
    Extrae mensajeros uniendo clientes_mensajeroaquitoy con auth_user
    para obtener el nombre completo, y mensajeria_tipovehiculo para el tipo.
    """
    return pd.read_sql_query("""
        SELECT
            m.id            AS mensajero_id,
            au.first_name || ' ' || au.last_name AS nombre,
            ci.nombre       AS ciudad_base,
            tv.nombre       AS tipo_vehiculo
        FROM clientes_mensajeroaquitoy m
        JOIN auth_user au ON au.id = m.user_id
        LEFT JOIN ciudad ci ON ci.ciudad_id = m.ciudad_operacion_id
        LEFT JOIN mensajeria_tipovehiculo tv
               ON tv.id = (
                   SELECT s.tipo_vehiculo_id
                   FROM mensajeria_servicio s
                   WHERE s.mensajero_id = m.id
                   LIMIT 1
               )
    """, source)


def extract_ciudad(source: Engine) -> pd.DataFrame:
    """Extrae ciudades con departamento (región queda como el departamento)."""
    return pd.read_sql_query("""
        SELECT c.ciudad_id, c.nombre AS nombre_ciudad,
               d.nombre AS departamento
        FROM ciudad c
        LEFT JOIN departamento d ON d.departamento_id = c.departamento_id
    """, source)


def extract_tipo_servicio(source: Engine) -> pd.DataFrame:
    """Extrae tipos de servicio."""
    return pd.read_sql_query("""
        SELECT id, nombre, descripcion
        FROM mensajeria_tiposervicio
    """, source)


def extract_estado_servicio(source: Engine) -> pd.DataFrame:
    """
    Extrae los estados del servicio.
    Los estados conocidos y su orden: 
      1-Iniciado, 2-Con mensajero asignado, 3-Recogido en origen,
      4-Entregado en destino, 5-Cerrado
    """
    return pd.read_sql_query("""
        SELECT id, nombre, descripcion
        FROM mensajeria_estado
        ORDER BY id
    """, source)


def extract_tipo_novedad(source: Engine) -> pd.DataFrame:
    """Extrae tipos de novedad."""
    return pd.read_sql_query("""
        SELECT id, nombre
        FROM mensajeria_tiponovedad
    """, source)


# ─────────────────────────────────────────────
#  EXTRACCIÓN PARA HECHOS (desde BD fuente)
# ─────────────────────────────────────────────

def extract_servicios(source: Engine) -> pd.DataFrame:
    """
    Extrae todos los servicios con sus datos completos.
    Incluye origen, destino, mensajero, cliente, tipo servicio.
    """
    return pd.read_sql_query("""
        SELECT
            s.id                    AS servicio_id,
            s.fecha_solicitud,
            s.hora_solicitud,
            s.cliente_id,
            s.mensajero_id,
            s.origen_id,
            s.destino_id,
            s.ciudad_origen_id,
            s.ciudad_destino_id,
            s.tipo_servicio_id,
            s.prioridad,
            s.novedades,
            os.ciudad_id            AS ciudad_origen_sede,
            ds.ciudad_id            AS ciudad_destino_sede
        FROM mensajeria_servicio s
        LEFT JOIN mensajeria_origenservicio os ON os.id = s.origen_id
        LEFT JOIN mensajeria_destinoservicio ds ON ds.id = s.destino_id
        WHERE s.es_prueba = FALSE
    """, source)


def extract_estados_servicio_detalle(source: Engine) -> pd.DataFrame:
    """
    Extrae todas las transiciones de estado de cada servicio,
    combinando fecha y hora en un solo timestamp.
    Se incluyen las filas de estado aunque estén marcadas como prueba,
    porque los estados iniciales de un servicio real pueden venir así.
    """
    return pd.read_sql_query("""
        SELECT
            es.id,
            es.servicio_id,
            es.estado_id,
            es.fecha,
            es.hora,
            (es.fecha::text || ' ' || es.hora::text)::timestamp AS timestamp_estado,
            es.observaciones
        FROM mensajeria_estadosservicio es
        ORDER BY es.servicio_id, es.fecha, es.hora
    """, source)


def extract_novedades(source: Engine) -> pd.DataFrame:
    """
    Extrae novedades de los servicios desde mensajeria_novedadesservicio.
    Columnas: id, fecha_novedad, tipo_novedad_id, descripcion,
              servicio_id, es_prueba, mensajero_id
    """
    return pd.read_sql_query("""
        SELECT
            n.id,
            n.fecha_novedad,
            n.tipo_novedad_id,
            n.descripcion,
            n.servicio_id,
            n.mensajero_id,
            t.nombre AS nombre_tipo_novedad
        FROM mensajeria_novedadesservicio n
        LEFT JOIN mensajeria_tiponovedad t ON t.id = n.tipo_novedad_id
        WHERE n.es_prueba = FALSE
    """, source)


# ─────────────────────────────────────────────
#  EXTRACCIÓN PARA HECHOS (desde DW ya cargado)
# ─────────────────────────────────────────────

def extract_dims_para_hecho_servicio(target: Engine) -> dict:
    """Lee las dimensiones ya cargadas en el DW para hacer los joins del hecho."""
    return {
        'dim_fecha':         pd.read_sql_table('dim_fecha',         target),
        'dim_cliente':       pd.read_sql_table('dim_cliente',       target),
        'dim_sede':          pd.read_sql_table('dim_sede',          target),
        'dim_mensajero':     pd.read_sql_table('dim_mensajero',     target),
        'dim_ciudad':        pd.read_sql_table('dim_ciudad',        target),
        'dim_tipo_servicio': pd.read_sql_table('dim_tipo_servicio', target),
    }


def extract_dims_para_hecho_transiciones(target: Engine) -> dict:
    """Lee las dimensiones para el hecho de transiciones."""
    return {
        'dim_fecha':           pd.read_sql_table('dim_fecha',           target),
        'dim_mensajero':       pd.read_sql_table('dim_mensajero',       target),
        'dim_cliente':         pd.read_sql_table('dim_cliente',         target),
        'dim_estado_servicio': pd.read_sql_table('dim_estado_servicio', target),
        'dim_ciudad':          pd.read_sql_table('dim_ciudad',          target),
        'dim_sede':            pd.read_sql_table('dim_sede',            target),
    }