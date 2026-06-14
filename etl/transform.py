"""
transform.py - Fast and Safe ETL
Transforma los datos extraídos para cargarlos en el DW.
"""

import holidays
import numpy as np
import pandas as pd
from datetime import date, datetime, timedelta
from pandas import DataFrame


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
    dim.drop_duplicates(subset=['cod_cliente'], inplace=True)
    dim.fillna('NO APLICA', inplace=True)
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
    dim.fillna('NO APLICA', inplace=True)
    return dim.reset_index(drop=True)


def transform_dim_mensajero(df: DataFrame) -> DataFrame:
    """Transforma mensajero para dim_mensajero."""
    dim = df[['mensajero_id', 'nombre', 'ciudad_base', 'tipo_vehiculo']].copy()
    dim.rename(columns={'mensajero_id': 'cod_mensajero'}, inplace=True)
    dim.fillna('NO APLICA', inplace=True)
    dim.drop_duplicates(subset=['cod_mensajero'], inplace=True)
    return dim.reset_index(drop=True)


def transform_dim_ciudad(df: DataFrame) -> DataFrame:
    """Transforma ciudad para dim_ciudad."""
    dim = df[['ciudad_id', 'nombre_ciudad', 'departamento']].copy()
    dim.rename(columns={'ciudad_id': 'cod_ciudad'}, inplace=True)
    # Region = departamento (simplificación válida para Colombia)
    dim['region'] = dim['departamento']
    dim.fillna('NO APLICA', inplace=True)
    dim.drop_duplicates(subset=['cod_ciudad'], inplace=True)
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

    dim['tiempo_max_horas'] = dim['nombre'].apply(tiempo_max)
    dim['urgente'] = dim['nombre'].apply(lambda x: 'urgente' in str(x).lower())
    dim.fillna({'descripcion': 'NO APLICA'}, inplace=True)
    return dim.reset_index(drop=True)


def transform_dim_estado_servicio(df: DataFrame) -> DataFrame:
    """
    Transforma mensajeria_estado para dim_estado_servicio.
    Asigna orden y si es estado final.
    """
    ORDEN = {
        'iniciado': 1,
        'con mensajero asignado': 2,
        'recogido en origen': 3,
        'entregado en destino': 4,
        'cerrado': 5,
        'cancelado': 6,
    }
    FINAL = {'cerrado', 'cancelado', 'entregado en destino'}

    dim = df[['id', 'nombre', 'descripcion']].copy()
    dim.rename(columns={'id': 'cod_estado', 'descripcion': 'descripcion'}, inplace=True)
    dim['nombre_estado'] = dim['nombre']
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

    df_srv = df_servicios.merge(pivot, left_on='servicio_id', right_on='servicio_id', how='left')

    # Detectar columnas de estado disponibles
    estado_cols = [c for c in df_srv.columns if c.startswith('estado_')]

    def minutos_entre(ts1, ts2):
        """Calcula minutos entre dos timestamps; retorna None si alguno es nulo."""
        if pd.isna(ts1) or pd.isna(ts2):
            return None
        diff = ts2 - ts1
        return round(diff.total_seconds() / 60, 2)

    # Asumimos: estado 1=Iniciado, 2=Asignado, 3=Recogido, 4=Entregado, 5=Cerrado
    # (ajusta los IDs si en tu BD son diferentes)
    e = lambda n: f'estado_{n}' if f'estado_{n}' in df_srv.columns else None

    ts_inicio   = df_srv[e(1)]   if e(1) else None
    ts_asignado = df_srv[e(2)]   if e(2) else None
    ts_recogido = df_srv[e(3)]   if e(3) else None
    ts_entregado= df_srv[e(4)]   if e(4) else None
    ts_cerrado  = df_srv[e(5)]   if e(5) else None

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

    # FK fecha entrega (último estado registrado)
    ultimo_estado = df_estados.sort_values('timestamp_estado') \
                              .groupby('servicio_id')['fecha'].last().reset_index()
    ultimo_estado['fecha_date'] = pd.to_datetime(ultimo_estado['fecha']).dt.date
    ultimo_estado = ultimo_estado.merge(
        dim_fecha[['key_dim_fecha', 'fecha']].rename(columns={'key_dim_fecha': 'fk_fecha_entrega'}),
        left_on='fecha_date', right_on='fecha', how='left'
    )[['servicio_id', 'fk_fecha_entrega']]
    df_srv = df_srv.merge(ultimo_estado, on='servicio_id', how='left')

    # FK cliente
    df_srv = df_srv.merge(
        dim_cliente[['key_dim_cliente', 'cod_cliente']].rename(columns={'key_dim_cliente': 'fk_cliente'}),
        left_on='cliente_id', right_on='cod_cliente', how='left'
    ).drop(columns=['cod_cliente'])

    # FK sede (origen)
    df_srv = df_srv.merge(
        dim_sede[['key_dim_sede', 'cod_sede']].rename(columns={'key_dim_sede': 'fk_sede'}),
        left_on='origen_id', right_on='cod_sede', how='left'
    ).drop(columns=['cod_sede'])

    # FK mensajero
    df_srv = df_srv.merge(
        dim_mensajero[['key_dim_mensajero', 'cod_mensajero']].rename(columns={'key_dim_mensajero': 'fk_mensajero'}),
        left_on='mensajero_id', right_on='cod_mensajero', how='left'
    ).drop(columns=['cod_mensajero'])

    # FK ciudad origen
    df_srv = df_srv.merge(
        dim_ciudad[['key_dim_ciudad', 'cod_ciudad']].rename(columns={'key_dim_ciudad': 'fk_ciudad_origen'}),
        left_on='ciudad_origen_id', right_on='cod_ciudad', how='left'
    ).drop(columns=['cod_ciudad'])

    # FK ciudad destino
    df_srv = df_srv.merge(
        dim_ciudad[['key_dim_ciudad', 'cod_ciudad']].rename(columns={'key_dim_ciudad': 'fk_ciudad_destino'}),
        left_on='ciudad_destino_id', right_on='cod_ciudad', how='left'
    ).drop(columns=['cod_ciudad'])

    # FK tipo servicio
    df_srv = df_srv.merge(
        dim_ts[['key_dim_tipo_servicio', 'cod_tipo_serv']].rename(columns={'key_dim_tipo_servicio': 'fk_tipo_servicio'}),
        left_on='tipo_servicio_id', right_on='cod_tipo_serv', how='left'
    ).drop(columns=['cod_tipo_serv'])

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

    df = df.merge(
        dim_mensajero[['key_dim_mensajero', 'cod_mensajero']].rename(columns={'key_dim_mensajero': 'fk_mensajero'}),
        left_on='mensajero_id', right_on='cod_mensajero', how='left'
    ).drop(columns=['cod_mensajero'], errors='ignore')

    df = df.merge(
        dim_cliente[['key_dim_cliente', 'cod_cliente']].rename(columns={'key_dim_cliente': 'fk_cliente'}),
        left_on='cliente_id', right_on='cod_cliente', how='left'
    ).drop(columns=['cod_cliente'], errors='ignore')

    df = df.merge(
        dim_estado[['key_dim_estado', 'cod_estado']].rename(columns={'key_dim_estado': 'fk_estado_servicio'}),
        left_on='estado_id', right_on='cod_estado', how='left'
    ).drop(columns=['cod_estado'], errors='ignore')

    df = df.merge(
        dim_ciudad[['key_dim_ciudad', 'cod_ciudad']].rename(columns={'key_dim_ciudad': 'fk_ciudad_origen'}),
        left_on='ciudad_origen_id', right_on='cod_ciudad', how='left'
    ).drop(columns=['cod_ciudad'], errors='ignore')

    df = df.merge(
        dim_ciudad[['key_dim_ciudad', 'cod_ciudad']].rename(columns={'key_dim_ciudad': 'fk_ciudad_destino'}),
        left_on='ciudad_destino_id', right_on='cod_ciudad', how='left'
    ).drop(columns=['cod_ciudad'], errors='ignore')

    df = df.merge(
        dim_sede[['key_dim_sede', 'cod_sede']].rename(columns={'key_dim_sede': 'fk_sede'}),
        left_on='origen_id', right_on='cod_sede', how='left'
    ).drop(columns=['cod_sede'], errors='ignore')

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