"""
main.py - Fast and Safe ETL
Orquesta el proceso completo: Extract → Transform → Load
para los dos data marts:
  1. hecho_servicio
  2. hecho_transiciones_estado
"""

import yaml
import pandas as pd
from sqlalchemy import create_engine

from etl import extract, transform, load, utils_etl

pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 50)


# ─────────────────────────────────────────────
#  1. CONFIGURACIÓN Y CONEXIONES
# ─────────────────────────────────────────────

with open('config.yml', 'r') as f:
    config = yaml.safe_load(f)

cfg_src = config['SOURCE_DB']
cfg_tgt = config['TARGET_DB']

url_source = (f"{cfg_src['drivername']}://{cfg_src['user']}:{cfg_src['password']}"
              f"@{cfg_src['host']}:{cfg_src['port']}/{cfg_src['dbname']}")

url_target = (f"{cfg_tgt['drivername']}://{cfg_tgt['user']}:{cfg_tgt['password']}"
              f"@{cfg_tgt['host']}:{cfg_tgt['port']}/{cfg_tgt['dbname']}")

source = create_engine(url_source)
target = create_engine(url_target)

print("=" * 60)
print("  Fast and Safe - ETL")
print("=" * 60)

# Verificar conexiones
utils_etl.test_connection(source, 'BD Operacional (fuente)')
utils_etl.test_connection(target, 'BD DW (destino)')

# Inicializar esquema DW si no existe
if not utils_etl.schema_exists(target):
    print("\n[INIT] Creando esquema del DW...")
    utils_etl.init_schema(target)
else:
    print("\n[INFO] Esquema DW ya existe.")


# ─────────────────────────────────────────────
#  2. EXTRACCIÓN (desde BD operacional)
# ─────────────────────────────────────────────

print("\n[EXTRACT] Extrayendo datos de la BD operacional...")

df_cliente       = extract.extract_cliente(source)
df_sede          = extract.extract_sede(source)
df_mensajero     = extract.extract_mensajero(source)
df_ciudad        = extract.extract_ciudad(source)
df_tipo_serv     = extract.extract_tipo_servicio(source)
df_estado        = extract.extract_estado_servicio(source)
df_tipo_nov      = extract.extract_tipo_novedad(source)
df_servicios     = extract.extract_servicios(source)
df_estados_det   = extract.extract_estados_servicio_detalle(source)
df_novedades     = extract.extract_novedades(source)

print("  [OK] Extracción completa.")


# ─────────────────────────────────────────────
#  3. TRANSFORMACIÓN DE DIMENSIONES
# ─────────────────────────────────────────────

print("\n[TRANSFORM] Transformando dimensiones...")

dim_fecha         = transform.transform_dim_fecha(df_servicios, df_estados_det)
dim_cliente       = transform.transform_dim_cliente(df_cliente)
dim_sede          = transform.transform_dim_sede(df_sede)
dim_mensajero     = transform.transform_dim_mensajero(df_mensajero)
dim_ciudad        = transform.transform_dim_ciudad(df_ciudad)
dim_tipo_servicio = transform.transform_dim_tipo_servicio(df_tipo_serv)
dim_estado_serv   = transform.transform_dim_estado_servicio(df_estado)
dim_tipo_novedad  = transform.transform_dim_tipo_novedad_completo(df_tipo_nov)
dim_hora          = transform.transform_dim_hora()

print("  [OK] Transformación de dimensiones completa.")


# ─────────────────────────────────────────────
#  4. CARGA DE DIMENSIONES
# ─────────────────────────────────────────────

if config.get('LOAD_DIMENSIONS', True):
    print("\n[LOAD] Cargando dimensiones en el DW...")

    load.load_dim(dim_fecha,         target, 'dim_fecha',         'key_dim_fecha')
    load.load_dim(dim_cliente,       target, 'dim_cliente',       'key_dim_cliente')
    load.load_dim(dim_sede,          target, 'dim_sede',          'key_dim_sede')
    load.load_dim(dim_mensajero,     target, 'dim_mensajero',     'key_dim_mensajero')
    load.load_dim(dim_ciudad,        target, 'dim_ciudad',        'key_dim_ciudad')
    load.load_dim(dim_tipo_servicio, target, 'dim_tipo_servicio', 'key_dim_tipo_servicio')
    load.load_dim(dim_estado_serv,   target, 'dim_estado_servicio','key_dim_estado')
    load.load_dim(dim_tipo_novedad,  target, 'dim_tipo_novedad',  'key_dim_tipo_novedad')
    load.load_dim(dim_hora,          target, 'dim_hora',          'key_dim_hora')

    print("  [OK] Todas las dimensiones cargadas.")
else:
    print("\n[INFO] LOAD_DIMENSIONS=False, leyendo dimensiones del DW...")


# ─────────────────────────────────────────────
#  5. TRANSFORMACIÓN Y CARGA DE HECHOS
# ─────────────────────────────────────────────

# ── 5a. Hecho Servicio ───────────────────────
print("\n[TRANSFORM] Construyendo hecho_servicio...")

dims_hecho_srv = extract.extract_dims_para_hecho_servicio(target)

hecho_servicio = transform.transform_hecho_servicio(
    df_servicios, df_estados_det, df_novedades, dims_hecho_srv
)

print("[LOAD] Cargando hecho_servicio...")
load.load(hecho_servicio, target, 'hecho_servicio', replace=True)


# ── 5b. Hecho Transiciones de Estado ─────────
print("\n[TRANSFORM] Construyendo hecho_transiciones_estado...")

dims_hecho_trans = extract.extract_dims_para_hecho_transiciones(target)

hecho_transiciones = transform.transform_hecho_transiciones(
    df_servicios, df_estados_det, dims_hecho_trans
)

print("[LOAD] Cargando hecho_transiciones_estado...")
load.load(hecho_transiciones, target, 'hecho_transiciones_estado', replace=True)


# ── 5c. Resumen de novedades (para pregunta 9 del enunciado) ─────────────────
print("\n[TRANSFORM] Construyendo resumen_novedades...")
resumen_nov = transform.resumen_novedades_por_tipo(df_novedades)
load.load(resumen_nov, target, 'resumen_novedades', replace=True)


# ─────────────────────────────────────────────
print("\n" + "=" * 60)
print("  ETL completado con éxito.")
print("=" * 60)