"""
transform.py - Fast and Safe ETL
Transforma los datos extraídos para cargarlos en el DW.
"""

import holidays
import numpy as np
import pandas as pd
from datetime import date, datetime, timedelta
from pandas import DataFrame


def _rellenar_valores_faltantes(dim: DataFrame, valor: str) -> DataFrame:
    """Rellena nulos conservando el tipo de las columnas numéricas."""
    dim = dim.copy()
    for col in dim.columns:
        if dim[col].isna().any():
            if pd.api.types.is_bool_dtype(dim[col]):
                dim[col] = dim[col].fillna(False)
            elif pd.api.types.is_numeric_dtype(dim[col]):
                dim[col] = dim[col].fillna(0)
            else:
                dim[col] = dim[col].fillna(valor)
    return dim


def _agregar_miembro_desconocido(
    dim: DataFrame,
    key_col: str,
    key_value: int = 0,
    valor_etiqueta: str = 'DESCONOCIDO'
) -> DataFrame:
    """Agrega una fila especial con key 0 para valores desconocidos."""
    dim = dim.copy()
    if key_col not in dim.columns:
        dim[key_col] = range(1, len(dim) + 1)

    if key_value in dim[key_col].astype(object).tolist():
        return dim

    row = {key_col: key_value}
    for col in dim.columns:
        if col == key_col:
            continue
        if pd.api.types.is_bool_dtype(dim[col]):
            row[col] = False
        elif pd.api.types.is_numeric_dtype(dim[col]):
            row[col] = 0
        else:
            row[col] = valor_etiqueta

    dim = pd.concat([dim, pd.DataFrame([row])], ignore_index=True)
    dim[key_col] = pd.to_numeric(dim[key_col], errors='coerce').fillna(0).astype(int)
    return dim


# ─────────────────────────────────────────────
#  DIMENSIONES
# ─────────────────────────────────────────────

def transform_dim_fecha(df_servicios: DataFrame, df_estados: DataFrame) -> DataFrame:
    """
    Genera dim_fecha a partir del rango de fechas encontradas en los servicios
    y sus transiciones de estado. Incluye festivos colombianos.
    """
    # Recolectar todas las fechas relevantes
    fechas_srv = pd.to_datetime(df_servicios['fecha_solicitud'], errors='coerce').dropna()
    fechas_est = pd.to_datetime(df_estados['fecha'], errors='coerce').dropna()
    
    fecha_min = min(fechas_srv.min(), fechas_est.min())
    fecha_max = max(fechas_srv.max(), fechas_est.max())

    # Generar rango diario
    dim_fecha = pd.DataFrame({
        'fecha': pd.date_range(start=fecha_min.date(), end=fecha_max.date(), freq='D')
    })
    dim_fecha.insert(0, 'key_dim_fecha', range(1, len(dim_fecha) + 1))

    dim_fecha['year']        = dim_fecha['fecha'].dt.year
    dim_fecha['month']       = dim_fecha['fecha'].dt.month
    dim_fecha['day']         = dim_fecha['fecha'].dt.day
    dim_fecha['hour']        = 0  # la hora se maneja aparte en los hechos
    dim_fecha['weekday']     = dim_fecha['fecha'].dt.weekday   # 0=lunes
    dim_fecha['quarter']     = dim_fecha['fecha'].dt.quarter
    dim_fecha['nombre_mes']  = dim_fecha['fecha'].dt.month_name(locale='es_CO.UTF-8') \
                                if _locale_disponible() else dim_fecha['fecha'].dt.month_name()
    dim_fecha['nombre_dia']  = dim_fecha['fecha'].dt.day_name(locale='es_CO.UTF-8') \
                                if _locale_disponible() else dim_fecha['fecha'].dt.day_name()
    dim_fecha['es_fin_semana'] = dim_fecha['weekday'] >= 5

    co_holidays = holidays.CO(language='es')
    dim_fecha['is_holiday']     = dim_fecha['fecha'].apply(lambda x: x in co_holidays)
    dim_fecha['nombre_festivo'] = dim_fecha['fecha'].apply(lambda x: co_holidays.get(x, None))

    dim_fecha = _rellenar_valores_faltantes(dim_fecha, 'DESCONOCIDO')
    dim_fecha = _agregar_miembro_desconocido(dim_fecha, 'key_dim_fecha', key_value=0, valor_etiqueta='DESCONOCIDO')
    dim_fecha.loc[dim_fecha['key_dim_fecha'] == 0, 'fecha'] = pd.NaT
    dim_fecha.loc[dim_fecha['key_dim_fecha'] == 0, 'year'] = 0
    dim_fecha.loc[dim_fecha['key_dim_fecha'] == 0, 'month'] = 0
    dim_fecha.loc[dim_fecha['key_dim_fecha'] == 0, 'day'] = 0
    dim_fecha.loc[dim_fecha['key_dim_fecha'] == 0, 'hour'] = 0
    dim_fecha.loc[dim_fecha['key_dim_fecha'] == 0, 'weekday'] = 0
    dim_fecha.loc[dim_fecha['key_dim_fecha'] == 0, 'quarter'] = 0
    dim_fecha.loc[dim_fecha['key_dim_fecha'] == 0, 'es_fin_semana'] = False
    dim_fecha.loc[dim_fecha['key_dim_fecha'] == 0, 'is_holiday'] = False
    dim_fecha.loc[dim_fecha['key_dim_fecha'] == 0, 'nombre_mes'] = 'DESCONOCIDO'
    dim_fecha.loc[dim_fecha['key_dim_fecha'] == 0, 'nombre_dia'] = 'DESCONOCIDO'
    dim_fecha.loc[dim_fecha['key_dim_fecha'] == 0, 'nombre_festivo'] = 'DESCONOCIDO'

    dim_fecha.reset_index(drop=True, inplace=True)
    return dim_fecha


def transform_dim_cliente(df: DataFrame) -> DataFrame:
    """Transforma la tabla cliente para dim_cliente."""
    dim = df[['cliente_id', 'nit_cliente', 'nombre', 'sector']].copy()
    dim.rename(columns={
        'cliente_id':  'cod_cliente',
        'nombre':      'nombre_cliente',
        'sector':      'sector_economico',
    }, inplace=True)
    dim.insert(0, 'key_dim_cliente', range(1, len(dim) + 1))
    dim.drop_duplicates(subset=['cod_cliente'], inplace=True)
    dim = _rellenar_valores_faltantes(dim, 'NO APLICA')
    dim = _agregar_miembro_desconocido(dim, 'key_dim_cliente', key_value=0, valor_etiqueta='DESCONOCIDO')
    dim.loc[dim['cod_cliente'] == 0, 'nombre_cliente'] = 'DESCONOCIDO'
    dim.loc[dim['cod_cliente'] == 0, 'sector_economico'] = 'DESCONOCIDO'
    dim.loc[dim['cod_cliente'] == 0, 'nit_cliente'] = 'DESCONOCIDO'
    return dim.reset_index(drop=True)


def transform_dim_sede(df: DataFrame) -> DataFrame:
    """Transforma sede para dim_sede."""
    dim = df[['sede_id', 'nombre', 'direccion', 'nombre_ciudad', 'cliente_id']].copy()
    dim.rename(columns={
        'sede_id':      'cod_sede',
        'nombre':       'nombre_sede',
        'nombre_ciudad':'ciudad',
        'cliente_id':   'cod_cliente',
    }, inplace=True)
    dim.insert(0, 'key_dim_sede', range(1, len(dim) + 1))
    dim = _rellenar_valores_faltantes(dim, 'NO APLICA')
    dim = _agregar_miembro_desconocido(dim, 'key_dim_sede', key_value=0, valor_etiqueta='DESCONOCIDO')
    dim.loc[dim['cod_sede'] == 0, 'nombre_sede'] = 'SIN SEDE'
    dim.loc[dim['cod_sede'] == 0, 'ciudad'] = 'DESCONOCIDO'
    dim.loc[dim['cod_sede'] == 0, 'direccion'] = 'DESCONOCIDO'
    dim.loc[dim['cod_sede'] == 0, 'cod_cliente'] = 0
    return dim.reset_index(drop=True)


def transform_dim_mensajero(df: DataFrame) -> DataFrame:
    """Transforma mensajero para dim_mensajero."""
    dim = df[['mensajero_id', 'nombre', 'ciudad_base', 'tipo_vehiculo']].copy()
    dim.rename(columns={'mensajero_id': 'cod_mensajero'}, inplace=True)
    dim.insert(0, 'key_dim_mensajero', range(1, len(dim) + 1))
    dim = _rellenar_valores_faltantes(dim, 'NO APLICA')
    dim.drop_duplicates(subset=['cod_mensajero'], inplace=True)
    dim = _agregar_miembro_desconocido(dim, 'key_dim_mensajero', key_value=0, valor_etiqueta='DESCONOCIDO')
    dim.loc[dim['cod_mensajero'] == 0, 'nombre'] = 'SIN MENSAJERO ASIGNADO'
    dim.loc[dim['cod_mensajero'] == 0, 'ciudad_base'] = 'DESCONOCIDO'
    dim.loc[dim['cod_mensajero'] == 0, 'tipo_vehiculo'] = 'DESCONOCIDO'
    return dim.reset_index(drop=True)


def transform_dim_ciudad(df: DataFrame) -> DataFrame:
    """Transforma ciudad para dim_ciudad."""
    dim = df[['ciudad_id', 'nombre_ciudad', 'departamento']].copy()
    dim.rename(columns={'ciudad_id': 'cod_ciudad'}, inplace=True)
    dim.insert(0, 'key_dim_ciudad', range(1, len(dim) + 1))
    # Region = departamento (simplificación válida para Colombia)
    dim['region'] = dim['departamento']
    dim = _rellenar_valores_faltantes(dim, 'NO APLICA')
    dim.drop_duplicates(subset=['cod_ciudad'], inplace=True)
    dim = _agregar_miembro_desconocido(dim, 'key_dim_ciudad', key_value=0, valor_etiqueta='DESCONOCIDO')
    dim.loc[dim['cod_ciudad'] == 0, 'nombre_ciudad'] = 'DESCONOCIDO'
    dim.loc[dim['cod_ciudad'] == 0, 'departamento'] = 'DESCONOCIDO'
    dim.loc[dim['cod_ciudad'] == 0, 'region'] = 'DESCONOCIDO'
    return dim.reset_index(drop=True)


def transform_dim_tipo_servicio(df: DataFrame) -> DataFrame:
    """
    Transforma tiposervicio para dim_tipo_servicio.
    Calcula tiempo_max_horas y urgente desde el nombre/descripción.
    """
    dim = df[['id', 'nombre', 'descripcion']].copy()
    dim.rename(columns={
        'id':          'cod_tipo_serv',
        'descripcion': 'descripcion',
    }, inplace=True)

    # Inferir tiempo máximo y urgencia desde el nombre del tipo de servicio
    def tiempo_max(nombre):
        n = str(nombre).lower()
        if 'urgente' in n or '1 hora' in n or 'hora' in n:
            return 1.0
        elif '2' in n and '3' in n:
            return 3.0
        elif 'día' in n or 'dia' in n:
            return 8.0
        return None

    dim.insert(0, 'key_dim_tipo_servicio', range(1, len(dim) + 1))
    dim['tiempo_max_horas'] = dim['nombre'].apply(tiempo_max)
    dim['urgente'] = dim['nombre'].apply(lambda x: 'urgente' in str(x).lower())
    dim.fillna({'descripcion': 'NO APLICA'}, inplace=True)
    dim = _agregar_miembro_desconocido(dim, 'key_dim_tipo_servicio', key_value=0, valor_etiqueta='DESCONOCIDO')
    dim.loc[dim['key_dim_tipo_servicio'] == 0, 'cod_tipo_serv'] = 0
    dim.loc[dim['key_dim_tipo_servicio'] == 0, 'nombre'] = 'DESCONOCIDO'
    dim.loc[dim['key_dim_tipo_servicio'] == 0, 'descripcion'] = 'DESCONOCIDO'
    dim.loc[dim['key_dim_tipo_servicio'] == 0, 'tiempo_max_horas'] = 0
    dim.loc[dim['key_dim_tipo_servicio'] == 0, 'urgente'] = False
    return dim.reset_index(drop=True)


def transform_dim_estado_servicio(df: DataFrame) -> DataFrame:
    """
    Transforma mensajeria_estado para dim_estado_servicio.
    Asigna orden y si es estado final.
    """
    ORDEN = {
        'iniciado': 1,
        'con mensajero asignado': 2,
        'con novedad': 3,
        'recogido por mensajero': 4,
        'entregado en destino': 5,
        'terminado completo': 6,
    }
    FINAL = {'terminado completo', 'entregado en destino'}

    dim = df[['id', 'nombre', 'descripcion']].copy()
    dim.rename(columns={'id': 'cod_estado', 'descripcion': 'descripcion'}, inplace=True)
    dim.insert(0, 'key_dim_estado', range(1, len(dim) + 1))
    dim['nombre_estado'] = dim['nombre']
    dim = _agregar_miembro_desconocido(dim, 'key_dim_estado', key_value=0, valor_etiqueta='DESCONOCIDO')
    dim.loc[dim['key_dim_estado'] == 0, 'cod_estado'] = 0
    dim.loc[dim['key_dim_estado'] == 0, 'nombre_estado'] = 'DESCONOCIDO'
    dim['orden_estado']  = dim['nombre'].apply(
        lambda x: ORDEN.get(str(x).lower().strip(), 99)
    )
    dim['es_estado_final'] = dim['nombre'].apply(
        lambda x: str(x).lower().strip() in FINAL
    )
    dim.drop(columns=['nombre', 'descripcion'], inplace=True)
    return dim.reset_index(drop=True)


def transform_dim_tipo_novedad(df: DataFrame) -> DataFrame:
    """Transforma tiponovedad para dim_tipo_novedad."""
    dim = df[['id', 'nombre']].copy()
    dim.rename(columns={'id': 'cod_tipo_novedad'}, inplace=True)
    dim['categoria'] = 'GENERAL'  # si no hay campo categoría en la fuente
    dim.fillna('NO APLICA', inplace=True)
    return dim.reset_index(drop=True)


def transform_dim_hora() -> DataFrame:
    """
    Genera dim_hora con 24 horas del día.
    Incluye período del día y si es horario laboral.
    """
    dim = pd.DataFrame({'hora': range(24)})
    
    def periodo(h):
        if 0 <= h < 6:
            return 'MADRUGADA'
        elif 6 <= h < 12:
            return 'MAÑANA'
        elif 12 <= h < 18:
            return 'TARDE'
        else:
            return 'NOCHE'
    
    dim['nombre_periodo'] = dim['hora'].apply(periodo)
    dim['es_horario_laboral'] = dim['hora'].apply(lambda h: 6 <= h < 22)
    return dim.reset_index(drop=True)


# ─────────────────────────────────────────────
#  HECHO SERVICIO
# ─────────────────────────────────────────────

def transform_hecho_servicio(
    df_servicios: DataFrame,
    df_estados: DataFrame,
    df_novedades: DataFrame,
    dims: dict
) -> DataFrame:
    """
    Construye hecho_servicio.
    Para cada servicio calcula:
      - tiempo_total_minutos: desde solicitud hasta estado Cerrado/Entregado
      - tiempo_fase_asignacion_min: Iniciado → Con mensajero asignado
      - tiempo_fase_recogida_min: Con mensajero asignado → Recogido en origen
      - tiempo_fase_entrega_min: Recogido en origen → Entregado/Cerrado
      - num_novedades: cantidad de novedades registradas
    """
    dim_fecha    = dims['dim_fecha']
    dim_cliente  = dims['dim_cliente']
    dim_sede     = dims['dim_sede']
    dim_mensajero= dims['dim_mensajero']
    dim_ciudad   = dims['dim_ciudad']
    dim_ts       = dims['dim_tipo_servicio']

    # Convertir fechas
    df_servicios['fecha_solicitud'] = pd.to_datetime(df_servicios['fecha_solicitud'])
    df_estados['timestamp_estado']  = pd.to_datetime(df_estados['timestamp_estado'])
    df_estados['fecha']             = pd.to_datetime(df_estados['fecha'])

    # ── Tiempos por fase ──────────────────────────────────────────────────────
    # Pivotear: para cada servicio, obtener el timestamp de cada estado
    pivot = df_estados.sort_values('timestamp_estado') \
                      .groupby(['servicio_id', 'estado_id'])['timestamp_estado'] \
                      .first().unstack('estado_id')
    pivot.columns = [f'estado_{int(c)}' for c in pivot.columns]
    pivot.reset_index(inplace=True)

    # Debug temporal del pivot: revisar columnas y muestra inicial
    # print("Columnas del pivot:", pivot.columns.tolist())
    # print("Primeras filas:")
    # print(pivot.head(3))

    df_srv = df_servicios.merge(pivot, left_on='servicio_id', right_on='servicio_id', how='left')

    # Detectar columnas de estado disponibles
    estado_cols = [c for c in df_srv.columns if c.startswith('estado_')]

    def minutos_entre(ts1, ts2):
        """Calcula minutos entre dos timestamps; retorna None si alguno es nulo."""
        if pd.isna(ts1) or pd.isna(ts2):
            return None
        diff = ts2 - ts1
        return round(diff.total_seconds() / 60, 2)

    # Mapeo de estados según la BD operacional
    e = lambda n: f'estado_{n}' if f'estado_{n}' in df_srv.columns else None

    ts_inicio    = df_srv[e(1)]   if e(1) else None
    ts_asignado  = df_srv[e(2)]   if e(2) else None
    ts_recogido  = df_srv[e(4)]   if e(4) else None
    ts_entregado = df_srv[e(5)]   if e(5) else None
    ts_cerrado   = df_srv[e(6)]   if e(6) else None

    # Timestamp de inicio = fecha_solicitud + hora_solicitud
    df_srv['ts_solicitud'] = pd.to_datetime(
        df_srv['fecha_solicitud'].astype(str) + ' ' + df_srv['hora_solicitud'].astype(str),
        errors='coerce'
    )

    ts_fin = ts_cerrado if ts_cerrado is not None else ts_entregado

    df_srv['tiempo_total_minutos']       = [minutos_entre(a, b) for a, b in
                                            zip(df_srv['ts_solicitud'], ts_fin if ts_fin is not None else [None]*len(df_srv))]
    df_srv['tiempo_fase_asignacion_min'] = [minutos_entre(a, b) for a, b in
                                            zip(ts_inicio if ts_inicio is not None else [None]*len(df_srv),
                                                ts_asignado if ts_asignado is not None else [None]*len(df_srv))]
    df_srv['tiempo_fase_recogida_min']   = [minutos_entre(a, b) for a, b in
                                            zip(ts_asignado if ts_asignado is not None else [None]*len(df_srv),
                                                ts_recogido if ts_recogido is not None else [None]*len(df_srv))]
    df_srv['tiempo_fase_entrega_min']    = [minutos_entre(a, b) for a, b in
                                            zip(ts_recogido if ts_recogido is not None else [None]*len(df_srv),
                                                ts_fin if ts_fin is not None else [None]*len(df_srv))]

    for col in ['tiempo_total_minutos', 'tiempo_fase_asignacion_min',
                'tiempo_fase_recogida_min', 'tiempo_fase_entrega_min']:
        df_srv[col] = df_srv[col].fillna(0.001)

    # ── Número de novedades ───────────────────────────────────────────────────
    # Tabla fuente: mensajeria_novedadesservicio
    # Columnas clave: servicio_id, tipo_novedad_id, fecha_novedad, mensajero_id
    if not df_novedades.empty and 'servicio_id' in df_novedades.columns:
        nov_count = df_novedades.groupby('servicio_id').size().reset_index(name='num_novedades')
        df_srv = df_srv.merge(nov_count, on='servicio_id', how='left')
        df_srv['num_novedades'] = df_srv['num_novedades'].fillna(0).astype(int)
    else:
        df_srv['num_novedades'] = 0

    # ── Joins con dimensiones ─────────────────────────────────────────────────
    dim_fecha['fecha'] = pd.to_datetime(dim_fecha['fecha']).dt.date

    # FK fecha solicitud
    df_srv['fecha_sol_date'] = df_srv['fecha_solicitud'].dt.date
    df_srv = df_srv.merge(
        dim_fecha[['key_dim_fecha', 'fecha']].rename(columns={'key_dim_fecha': 'fk_fecha_solicitud'}),
        left_on='fecha_sol_date', right_on='fecha', how='left'
    ).drop(columns=['fecha', 'fecha_sol_date'])
    df_srv['fk_fecha_solicitud'] = df_srv['fk_fecha_solicitud'].fillna(0)

    # FK fecha entrega (último estado registrado)
    ultimo_estado = df_estados.sort_values('timestamp_estado') \
                              .groupby('servicio_id')['fecha'].last().reset_index()
    ultimo_estado['fecha_date'] = pd.to_datetime(ultimo_estado['fecha']).dt.date
    ultimo_estado = ultimo_estado.merge(
        dim_fecha[['key_dim_fecha', 'fecha']].rename(columns={'key_dim_fecha': 'fk_fecha_entrega'}),
        left_on='fecha_date', right_on='fecha', how='left'
    )[['servicio_id', 'fk_fecha_entrega']]
    ultimo_estado['fk_fecha_entrega'] = ultimo_estado['fk_fecha_entrega'].fillna(0)
    df_srv = df_srv.merge(ultimo_estado, on='servicio_id', how='left')
    df_srv['fk_fecha_entrega'] = df_srv['fk_fecha_entrega'].fillna(0).astype(int)

    df_srv['cliente_id'] = df_srv['cliente_id'].fillna(0)

    # FK cliente
    df_srv = df_srv.merge(
        dim_cliente[['key_dim_cliente', 'cod_cliente']].rename(columns={'key_dim_cliente': 'fk_cliente'}),
        left_on='cliente_id', right_on='cod_cliente', how='left'
    ).drop(columns=['cod_cliente'])
    df_srv['fk_cliente'] = df_srv['fk_cliente'].fillna(0)

    df_srv['origen_id'] = df_srv['origen_id'].fillna(0)

    # FK sede (origen)
    df_srv = df_srv.merge(
        dim_sede[['key_dim_sede', 'cod_sede']].rename(columns={'key_dim_sede': 'fk_sede'}),
        left_on='origen_id', right_on='cod_sede', how='left'
    ).drop(columns=['cod_sede'])
    df_srv['fk_sede'] = df_srv['fk_sede'].fillna(0)

    df_srv['mensajero_id'] = df_srv['mensajero_id'].fillna(0)

    # FK mensajero
    df_srv = df_srv.merge(
        dim_mensajero[['key_dim_mensajero', 'cod_mensajero']].rename(columns={'key_dim_mensajero': 'fk_mensajero'}),
        left_on='mensajero_id', right_on='cod_mensajero', how='left'
    ).drop(columns=['cod_mensajero'])
    df_srv['fk_mensajero'] = df_srv['fk_mensajero'].fillna(0)

    df_srv['ciudad_origen_id'] = df_srv['ciudad_origen_id'].fillna(0)

    # FK ciudad origen
    df_srv = df_srv.merge(
        dim_ciudad[['key_dim_ciudad', 'cod_ciudad']].rename(columns={'key_dim_ciudad': 'fk_ciudad_origen'}),
        left_on='ciudad_origen_id', right_on='cod_ciudad', how='left'
    ).drop(columns=['cod_ciudad'])
    df_srv['fk_ciudad_origen'] = df_srv['fk_ciudad_origen'].fillna(0)

    df_srv['ciudad_destino_id'] = df_srv['ciudad_destino_id'].fillna(0)

    # FK ciudad destino
    df_srv = df_srv.merge(
        dim_ciudad[['key_dim_ciudad', 'cod_ciudad']].rename(columns={'key_dim_ciudad': 'fk_ciudad_destino'}),
        left_on='ciudad_destino_id', right_on='cod_ciudad', how='left'
    ).drop(columns=['cod_ciudad'])
    df_srv['fk_ciudad_destino'] = df_srv['fk_ciudad_destino'].fillna(0)

    df_srv['tipo_servicio_id'] = df_srv['tipo_servicio_id'].fillna(0)

    # FK tipo servicio
    df_srv = df_srv.merge(
        dim_ts[['key_dim_tipo_servicio', 'cod_tipo_serv']].rename(columns={'key_dim_tipo_servicio': 'fk_tipo_servicio'}),
        left_on='tipo_servicio_id', right_on='cod_tipo_serv', how='left'
    ).drop(columns=['cod_tipo_serv'])
    df_srv['fk_tipo_servicio'] = df_srv['fk_tipo_servicio'].fillna(0)

    # ── Seleccionar columnas finales ──────────────────────────────────────────
    cols_finales = [
        'fk_fecha_solicitud', 'fk_fecha_entrega',
        'fk_cliente', 'fk_sede', 'fk_mensajero',
        'fk_ciudad_origen', 'fk_ciudad_destino', 'fk_tipo_servicio',
        'servicio_id',
        'tiempo_total_minutos', 'tiempo_fase_asignacion_min',
        'tiempo_fase_recogida_min', 'tiempo_fase_entrega_min',
        'num_novedades',
    ]
    hecho = df_srv[[c for c in cols_finales if c in df_srv.columns]].copy()
    hecho.rename(columns={'servicio_id': 'id_servicio'}, inplace=True)
    return hecho.reset_index(drop=True)


# ─────────────────────────────────────────────
#  HECHO TRANSICIONES DE ESTADO
# ─────────────────────────────────────────────

def transform_hecho_transiciones(
    df_servicios: DataFrame,
    df_estados: DataFrame,
    dims: dict
) -> DataFrame:
    """
    Construye hecho_transiciones_estado.
    Una fila por cada cambio de estado de cada servicio.
    Calcula duracion_en_estado_min = tiempo hasta el siguiente estado.
    """
    dim_fecha   = dims['dim_fecha']
    dim_mensajero = dims['dim_mensajero']
    dim_cliente   = dims['dim_cliente']
    dim_estado    = dims['dim_estado_servicio']
    dim_ciudad    = dims['dim_ciudad']
    dim_sede      = dims['dim_sede']

    df_estados = df_estados.copy()
    df_servicios = df_servicios.copy()
    df_estados['timestamp_estado'] = pd.to_datetime(df_estados['timestamp_estado'])

    # Calcular duración en cada estado (diferencia con el siguiente timestamp del mismo servicio)
    df_estados.sort_values(['servicio_id', 'timestamp_estado'], inplace=True)
    df_estados['ts_siguiente'] = df_estados.groupby('servicio_id')['timestamp_estado'].shift(-1)
    df_estados['duracion_en_estado_min'] = (
        (df_estados['ts_siguiente'] - df_estados['timestamp_estado']).dt.total_seconds() / 60
    ).round(2)

    # Unir con servicios para obtener cliente, mensajero, ciudades, sede
    df = df_estados.merge(
        df_servicios[['servicio_id', 'cliente_id', 'mensajero_id',
                      'ciudad_origen_id', 'ciudad_destino_id', 'origen_id']],
        on='servicio_id', how='left'
    )

    # ── Joins con dimensiones ─────────────────────────────────────────────────
    dim_fecha['fecha'] = pd.to_datetime(dim_fecha['fecha']).dt.date
    df['fecha_date'] = pd.to_datetime(df['fecha']).dt.date

    df = df.merge(
        dim_fecha[['key_dim_fecha', 'fecha']].rename(columns={'key_dim_fecha': 'fk_fecha_transicion'}),
        left_on='fecha_date', right_on='fecha', how='left'
    ).drop(columns=['fecha', 'fecha_date'], errors='ignore')
    df['fk_fecha_transicion'] = df['fk_fecha_transicion'].fillna(0)

    df['mensajero_id'] = df['mensajero_id'].fillna(0)
    df = df.merge(
        dim_mensajero[['key_dim_mensajero', 'cod_mensajero']].rename(columns={'key_dim_mensajero': 'fk_mensajero'}),
        left_on='mensajero_id', right_on='cod_mensajero', how='left'
    ).drop(columns=['cod_mensajero'], errors='ignore')
    df['fk_mensajero'] = df['fk_mensajero'].fillna(0)

    df['cliente_id'] = df['cliente_id'].fillna(0)
    df = df.merge(
        dim_cliente[['key_dim_cliente', 'cod_cliente']].rename(columns={'key_dim_cliente': 'fk_cliente'}),
        left_on='cliente_id', right_on='cod_cliente', how='left'
    ).drop(columns=['cod_cliente'], errors='ignore')
    df['fk_cliente'] = df['fk_cliente'].fillna(0)

    df['estado_id'] = df['estado_id'].fillna(0)
    df = df.merge(
        dim_estado[['key_dim_estado', 'cod_estado']].rename(columns={'key_dim_estado': 'fk_estado_servicio'}),
        left_on='estado_id', right_on='cod_estado', how='left'
    ).drop(columns=['cod_estado'], errors='ignore')
    df['fk_estado_servicio'] = df['fk_estado_servicio'].fillna(0)

    df['ciudad_origen_id'] = df['ciudad_origen_id'].fillna(0)
    df = df.merge(
        dim_ciudad[['key_dim_ciudad', 'cod_ciudad']].rename(columns={'key_dim_ciudad': 'fk_ciudad_origen'}),
        left_on='ciudad_origen_id', right_on='cod_ciudad', how='left'
    ).drop(columns=['cod_ciudad'], errors='ignore')
    df['fk_ciudad_origen'] = df['fk_ciudad_origen'].fillna(0)

    df['ciudad_destino_id'] = df['ciudad_destino_id'].fillna(0)
    df = df.merge(
        dim_ciudad[['key_dim_ciudad', 'cod_ciudad']].rename(columns={'key_dim_ciudad': 'fk_ciudad_destino'}),
        left_on='ciudad_destino_id', right_on='cod_ciudad', how='left'
    ).drop(columns=['cod_ciudad'], errors='ignore')
    df['fk_ciudad_destino'] = df['fk_ciudad_destino'].fillna(0)

    df['origen_id'] = df['origen_id'].fillna(0)
    df = df.merge(
        dim_sede[['key_dim_sede', 'cod_sede']].rename(columns={'key_dim_sede': 'fk_sede'}),
        left_on='origen_id', right_on='cod_sede', how='left'
    ).drop(columns=['cod_sede'], errors='ignore')
    df['fk_sede'] = df['fk_sede'].fillna(0)

    df['duracion_en_estado_min'] = df['duracion_en_estado_min'].fillna(0.001)

    # ── Columnas finales ──────────────────────────────────────────────────────
    cols = [
        'fk_fecha_transicion', 'fk_mensajero', 'fk_cliente',
        'fk_estado_servicio', 'fk_ciudad_origen', 'fk_ciudad_destino', 'fk_sede',
        'servicio_id', 'timestamp_estado', 'duracion_en_estado_min',
    ]
    hecho = df[[c for c in cols if c in df.columns]].copy()
    hecho.rename(columns={'servicio_id': 'id_servicio'}, inplace=True)
    return hecho.reset_index(drop=True)


# ─────────────────────────────────────────────
#  UTILIDADES INTERNAS
# ─────────────────────────────────────────────

def transform_dim_tipo_novedad_completo(df: DataFrame) -> DataFrame:
    """
    Transforma mensajeria_tiponovedad para dim_tipo_novedad.
    La tabla fuente solo tiene: id, nombre (varchar 30)
    Asignamos categorías manuales basadas en el nombre.
    """
    dim = df[['id', 'nombre']].copy()
    dim.rename(columns={'id': 'cod_tipo_novedad'}, inplace=True)

    def categorizar(nombre):
        n = str(nombre).lower()
        if any(p in n for p in ['moto', 'vehículo', 'vehiculo', 'daño', 'accidente', 'llanta', 'avería']):
            return 'VEHICULO'
        elif any(p in n for p in ['cliente', 'empaque', 'demora', 'espera']):
            return 'CLIENTE'
        elif any(p in n for p in ['tráfico', 'trafico', 'vía', 'via', 'cierre']):
            return 'VIALIDAD'
        elif any(p in n for p in ['mensajero', 'personal', 'enfermedad']):
            return 'MENSAJERO'
        return 'OTRO'

    dim['categoria'] = dim['nombre'].apply(categorizar)
    dim.fillna('NO APLICA', inplace=True)
    return dim.reset_index(drop=True)


def resumen_novedades_por_tipo(df_novedades: DataFrame) -> DataFrame:
    """
    Genera un resumen de cuántas veces ocurre cada tipo de novedad.
    Útil para responder la pregunta 9 del enunciado.
    Columnas fuente usadas: tipo_novedad_id, nombre_tipo_novedad, servicio_id, fecha_novedad
    """
    if df_novedades.empty:
        return pd.DataFrame(columns=['tipo_novedad_id', 'nombre_tipo_novedad', 'total_ocurrencias'])

    df_novedades['fecha_novedad'] = pd.to_datetime(df_novedades['fecha_novedad'])

    resumen = (
        df_novedades
        .groupby(['tipo_novedad_id', 'nombre_tipo_novedad'])
        .agg(
            total_ocurrencias=('id', 'count'),
            servicios_afectados=('servicio_id', 'nunique'),
        )
        .reset_index()
        .sort_values('total_ocurrencias', ascending=False)
    )
    return resumen


def _locale_disponible() -> bool:
    try:
        import locale
        locale.setlocale(locale.LC_TIME, 'es_CO.UTF-8')
        return True
    except Exception:
        return False