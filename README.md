# Fast and Safe - ETL Data Warehouse

Sistema ETL que extrae datos de la BD operacional de Fast and Safe y los carga en un data warehouse PostgreSQL para análisis.

## 📋 Requisitos Previos

- Python 3.9+
- PostgreSQL 12+
- pgAdmin (opcional, para administración)

## 🚀 Instalación y Ejecución

### 1. Crear la base de datos

En pgAdmin, ejecuta en Query Tool:

```sql
CREATE DATABASE fast_and_safe_dw WITH ENCODING 'UTF8';
```

Luego (una sola vez, la primera vez):

```sql
DROP SCHEMA IF EXISTS public CASCADE;
CREATE SCHEMA public;
```

### 2. Configurar credenciales

Edita `config.yml` con tus datos de conexión:

```yaml
SOURCE_DB:
  drivername: postgresql
  user: tu_usuario
  password: tu_contraseña
  host: localhost
  port: 5432
  dbname: BD_operacional

TARGET_DB:
  drivername: postgresql
  user: tu_usuario
  password: tu_contraseña
  host: localhost
  port: 5432
  dbname: fast_and_safe_dw
```

### 3. Instalar dependencias

```bash
pip install -r requirements.txt
```

### 4. Ejecutar el ETL

```bash
python main.py
```

Verás mensajes confirmando que cada dimensión y tabla de hechos se cargó correctamente.

---

## 🔄 Lógica Central

### **Las dimensiones se cargan antes que los hechos**

Esto es **crítico**. Los hechos necesitan los surrogate keys de las dimensiones para hacer JOINs:

```
dim_cliente → Proporciona key_dim_cliente
dim_fecha   → Proporciona key_dim_fecha
dim_mensajero → Proporciona key_dim_mensajero
        ↓
hecho_servicio usa: fk_cliente, fk_fecha, fk_mensajero (reemplazando los IDs operacionales)
```

### **Cómo se construye `hecho_servicio`**

1. **Pivotea estados**: Convierte la tabla de cambios de estado de formato largo a ancho (una columna por estado)
2. **Calcula tiempos**: Resta timestamps entre estados consecutivos para obtener duración en cada fase
3. **Reemplaza IDs**: Hace JOINs con dimensiones para reemplazar `cliente_id=5` por `fk_cliente=3`
4. **Resultado**: Una fila por servicio con tiempos de cada fase (asignación, recogida, entrega)

### **Cómo se construye `hecho_transiciones_estado`**

1. **Mantiene estructura original**: Una fila por cambio de estado (no pivotea)
2. **Calcula duración**: Resta el timestamp actual del siguiente timestamp del mismo servicio
3. **Resultado**: Una tabla para analizar "¿En qué fase del servicio hay más demoras?"

---

## 📁 Estructura

```
fast_and_safe_ETL/
├── config.yml              # Credenciales (NO subir a Git)
├── main.py                 # Orquestador del ETL
├── requirements.txt        # Dependencias
├── sqlscripts.yml          # Esquema del DW
├── README.md               # Este archivo
├── .gitignore              # Archivos a ignorar
├── etl/
│   ├── extract.py          # Extrae datos
│   ├── transform.py        # Transforma dimensiones y hechos
│   ├── load.py             # Carga en DW
│   └── utils_etl.py        # Funciones auxiliares
└── notebooks/
    └── validacion_dw.ipynb # Queries de validación
```

---



