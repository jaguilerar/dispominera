from flask import Flask, render_template, request, jsonify, send_file
from flask_httpauth import HTTPBasicAuth
from flask_caching import Cache
from werkzeug.security import generate_password_hash, check_password_hash
from datetime import datetime, timedelta
import os
import pandas as pd
import numpy as np
from dotenv import load_dotenv
import hashlib
import json
import itertools
from io import BytesIO
import openpyxl
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils.dataframe import dataframe_to_rows

load_dotenv()

# Intentar importar pyathena, pero permitir que la app funcione sin él
try:
    from pyathena import connect
    from pyathena.pandas.cursor import PandasCursor
    ATHENA_AVAILABLE = True
except ImportError:
    ATHENA_AVAILABLE = False
    print("⚠️ pyathena no disponible. La app usará datos de ejemplo de SQLite.")

app = Flask(__name__)
app.config['SECRET_KEY'] = os.getenv('SECRET_KEY', '')

# ============================================
# CONFIGURACIÓN DE CACHÉ
# ============================================
# Configurar cache - usar SimpleCache para desarrollo, Redis para producción
cache_config = {
    'CACHE_TYPE': os.getenv('CACHE_TYPE', 'SimpleCache'),  # 'SimpleCache' o 'RedisCache'
    'CACHE_DEFAULT_TIMEOUT': int(os.getenv('CACHE_TIMEOUT', '900')),  # 15 minutos por defecto
}

# Si se usa Redis, agregar configuración
if cache_config['CACHE_TYPE'] == 'RedisCache':
    cache_config.update({
        'CACHE_REDIS_URL': os.getenv('REDIS_URL', 'redis://localhost:6379/0')
    })

app.config.update(cache_config)
cache = Cache(app)

print(f"📦 Caché configurado: {cache_config['CACHE_TYPE']} (timeout: {cache_config['CACHE_DEFAULT_TIMEOUT']}s)")

# ============================================
# MIDDLEWARE PARA MEDICIÓN DE TIEMPOS
# ============================================
@app.before_request
def before_request():
    """Registrar tiempo de inicio de request"""
    from flask import g
    import time
    g.start_time = time.time()

@app.after_request
def after_request(response):
    """Registrar tiempo de respuesta"""
    from flask import g, request
    import time
    
    if hasattr(g, 'start_time'):
        elapsed = time.time() - g.start_time
        if request.path.startswith('/api/'):
            print(f"⏱️  {request.method} {request.path} - {elapsed:.2f}s")
    
    return response

# ============================================
# CONFIGURACIÓN DE AUTENTICACIÓN
# ============================================
auth = HTTPBasicAuth()

# Usuarios permitidos (desde variables de entorno)
USERS = {
    os.environ.get('APP_USER'): generate_password_hash(
        os.environ.get('APP_PASSWORD')
    )
}

@auth.verify_password
def verify_password(username, password):
    """Verificar credenciales de usuario"""
    if username in USERS and check_password_hash(USERS.get(username), password):
        return username
    return None
# ============================================
# FIN CONFIGURACIÓN DE AUTENTICACIÓN
# ============================================


# Configuración de AWS Athena (usar variables de entorno en producción)
AWS_CONFIG = {
    'aws_access_key_id': os.getenv('AWS_ACCESS_KEY', ''),
    'aws_secret_access_key': os.getenv('AWS_SECRET_KEY', ''),
    's3_staging_dir': os.getenv('S3_BUCKET', ''),
    'region_name': os.getenv('AWS_REGION', '')
}

# Nombre base de datos/tabla en Athena (desde variables de entorno)
DATABASE_ATHENA = os.getenv('DATABASE_ATHENA', '')
TABLA_PEDIDOS = os.getenv('TABLA_PEDIDOS', '')

# Configuración adicional para el análisis completo
DATABASE_DISPOMATE = os.getenv('DATABASE_DISPOMATE', '')
TABLA_TURNOS = os.getenv('TABLA_TURNOS', '')
TABLA_RCO = os.getenv('TABLA_RCO', '')
TABLA_FLOTA = os.getenv('TABLA_FLOTA', '')


# Forzar por defecto el uso de Athena si la librería está disponible.
# Si pyathena no está instalado, USE_ATHENA será False y la app informará al usuario.
USE_ATHENA = os.getenv('USE_ATHENA', 'true').lower() == 'true' and ATHENA_AVAILABLE

# Conexión global a Athena
athena_conn = None

# ============================================
# FUNCIONES HELPER PARA CACHÉ
# ============================================
def generar_cache_key(prefix, *args, **kwargs):
    """Generar clave única para cache basada en parámetros"""
    # Crear string único con todos los parámetros
    key_data = f"{prefix}_{args}_{sorted(kwargs.items())}"
    # Hash para mantener claves cortas
    key_hash = hashlib.md5(key_data.encode()).hexdigest()[:12]
    return f"{prefix}_{key_hash}"

def invalidar_cache_minera(minera_nombre):
    """Invalidar todo el cache relacionado con una minera específica"""
    # Flask-Caching no tiene invalidación por patrón en SimpleCache
    # Esta función es un placeholder para cuando se use Redis
    cache.clear()
    print(f"🗑️ Cache invalidado para minera: {minera_nombre}")

# ============================================
# FIN FUNCIONES HELPER PARA CACHÉ
# ============================================

def get_athena_connection():
    """Obtener o crear conexión a Athena"""
    global athena_conn
    if not USE_ATHENA or not ATHENA_AVAILABLE:
        return None
    
    if athena_conn is None:
        try:
            athena_conn = connect(
                cursor_class=PandasCursor,
                **AWS_CONFIG
            )
            print("✅ Conexión a Athena establecida")
        except Exception as e:
            print(f"❌ Error conectando a Athena: {e}")
            return None
    return athena_conn

def sql_athena(query):
    """Ejecutar query en Athena y retornar DataFrame"""
    try:
        conn = get_athena_connection()
        if conn:
            return conn.cursor().execute(query).as_pandas()
        raise RuntimeError("Athena no disponible: no se puede ejecutar queries")
    except Exception as e:
        print(f"Error ejecutando query en Athena: {e}")
        return pd.DataFrame()


@cache.memoize(timeout=900)  # Cache por 15 minutos
def obtener_datos_completos_athena(minera_nombre, fecha_inicio, fecha_fin):
    """
    Obtener datos completos integrando SCR, Turnos y RCO (similar al notebook)
    NOTA: Esta función está cacheada por 15 minutos (hace 3 queries grandes)
    """
    print(f"🔄 [CACHE MISS] Ejecutando 3 queries completas para {minera_nombre} ({fecha_inicio} - {fecha_fin})")
    
    if not USE_ATHENA:
        return None

    # 1. Obtener datos SCR (pedidos)
    df_scr = obtener_datos_desde_athena(minera_nombre, fecha_inicio, fecha_fin)
    if df_scr is None or df_scr.empty:
        return None

    # 2. Obtener turnos enviados
    # OPTIMIZACIÓN: Seleccionar solo columnas necesarias y filtrar en query
    query_turnos = f"""
    SELECT 
        id_turno_uuid,
        estado,
        id_vehiculo,
        fecha_inicio_utc,
        fecha_hora
    FROM {DATABASE_DISPOMATE}.{TABLA_TURNOS}
    WHERE DATE(fecha_inicio_utc) >= DATE('{fecha_inicio.strftime('%Y-%m-%d')}')
      AND DATE(fecha_inicio_utc) <= DATE('{fecha_fin.strftime('%Y-%m-%d')}')
    ORDER BY fecha_inicio_utc
    """
    
    try:
        df_turnos = sql_athena(query_turnos)
    except Exception as e:
        print(f"Error obteniendo turnos: {e}")
        df_turnos = pd.DataFrame()

    # 3. Obtener conexiones RCO
    # OPTIMIZACIÓN: Seleccionar solo columnas necesarias
    query_rco = f"""
    SELECT 
        codigo_tanque,
        fecha_conexion_utc
    FROM {DATABASE_DISPOMATE}.{TABLA_RCO}
    WHERE DATE(fecha_conexion_utc) >= DATE('{fecha_inicio.strftime('%Y-%m-%d')}')
      AND DATE(fecha_conexion_utc) <= DATE('{fecha_fin.strftime('%Y-%m-%d')}')
    ORDER BY fecha_conexion_utc
    """
    
    try:
        df_rco = sql_athena(query_rco)
    except Exception as e:
        print(f"Error obteniendo RCO: {e}")
        df_rco = pd.DataFrame()

    # 4. Procesar y combinar datos como en el notebook
    resultado_completo = procesar_datos_completos(df_scr, df_turnos, df_rco, fecha_inicio, fecha_fin, minera_nombre)
    
    return resultado_completo


def calcular_disponibilidad_operacional(datos_completos, banda_total=None):
    """
    Calcular la Disponibilidad Operacional según nueva métrica:
    
    Disponibilidad = (Entregas Exitosas) / (Banda Total del Transportista)
    
    Entregas Exitosas incluyen:
    - Criterio A: Pedido fue entregado correctamente (Entregado_totalmente > 0)
    - Criterio B: Pedido NO fue entregado PERO sí hubo conexión RCO (Entregado_totalmente == 0 AND Conexion_RCO > 0)
    
    Definiciones técnicas:
    - "Entregado Correctamente": Estado 'Entregado totalmente' o 'Recibí Conforme' en SCR
    - "Conexiones al RCO": Registro de conexión operacional en tabla RCO (cualquier evento > 0)
    - "Banda Total": Capacidad asignada al transportista (viajes programados por día)
    
    Args:
        datos_completos: DataFrame con columnas ['Turnos_enviados', 'Entregado_totalmente', 'Conexion_RCO', '¿Es licitado?']
        banda_total: Banda total del transportista (capacidad asignada). Si es None, se usa turnos_enviados como fallback.
    
    Returns:
        dict con:
            - entregas_exitosas_total: Total de entregas consideradas exitosas
            - entregas_criterio_a: Entregas completadas correctamente
            - entregas_criterio_b: Entregas fallidas pero con conexión RCO
            - turnos_enviados_total: Total de turnos enviados
            - banda_total: Banda total usada en el cálculo
            - disponibilidad_porcentaje: Porcentaje de disponibilidad operacional
            - entregas_licitadas: Total de entregas realizadas por flota licitada
            - entregas_totales: Total de entregas (Entregado_totalmente > 0)
            - disponibilidad_licitada_porcentaje: Porcentaje de entregas por flota licitada
    """
    if datos_completos is None or datos_completos.empty:
        return {
            'entregas_exitosas_total': 0,
            'entregas_criterio_a': 0,
            'entregas_criterio_b': 0,
            'turnos_enviados_total': 0,
            'disponibilidad_porcentaje': 0.0,
            'entregas_licitadas': 0,
            'entregas_totales': 0,
            'disponibilidad_licitada_porcentaje': 0.0
        }
    
    # Criterio A: Entregado correctamente
    entregas_criterio_a = int(datos_completos['Entregado_totalmente'].sum())
    
    # Criterio B: No entregado PERO con conexión RCO
    # IMPORTANTE: Contar solo 1 por registro si hubo RCO (no sumar todas las conexiones)
    # Identificar registros donde NO se entregó (Entregado_totalmente == 0) PERO hubo conexión RCO (Conexion_RCO > 0)
    criterio_b_mask = (datos_completos['Entregado_totalmente'] == 0) & (datos_completos['Conexion_RCO'] > 0)
    entregas_criterio_b = int(criterio_b_mask.sum())  # Cuenta 1 por cada registro que cumple la condición
    
    # Total de entregas exitosas (Criterio A ∪ Criterio B)
    entregas_exitosas_total = entregas_criterio_a + entregas_criterio_b
    
    # Total de turnos enviados
    turnos_enviados_total = int(datos_completos['Turnos_enviados'].sum())
    
    # Usar banda_total si está disponible, sino usar turnos_enviados como fallback
    denominador = banda_total if banda_total is not None and banda_total > 0 else turnos_enviados_total
    
    # Calcular porcentaje de disponibilidad operacional
    if denominador > 0:
        disponibilidad_porcentaje = (entregas_exitosas_total / denominador) * 100
        # IMPORTANTE: Limitar a 100% máximo
        disponibilidad_porcentaje = min(disponibilidad_porcentaje, 100.0)
    else:
        disponibilidad_porcentaje = 0.0
    
    # Calcular Disponibilidad Licitada
    # Entregas totales: suma de todas las entregas completadas
    entregas_totales = int(datos_completos['Entregado_totalmente'].sum())
    
    # Entregas licitadas: suma de entregas completadas por flota licitada
    entregas_licitadas = int(
        datos_completos[datos_completos['¿Es licitado?'] == 'Si']['Entregado_totalmente'].sum()
    )
    
    # Calcular porcentaje de disponibilidad licitada
    if entregas_totales > 0:
        disponibilidad_licitada_porcentaje = (entregas_licitadas / entregas_totales) * 100
    else:
        disponibilidad_licitada_porcentaje = 0.0
    
    return {
        'entregas_exitosas_total': entregas_exitosas_total,
        'entregas_criterio_a': entregas_criterio_a,
        'entregas_criterio_b': entregas_criterio_b,
        'turnos_enviados_total': turnos_enviados_total,
        'banda_total': denominador,
        'disponibilidad_porcentaje': round(disponibilidad_porcentaje, 1),
        'entregas_licitadas': entregas_licitadas,
        'entregas_totales': entregas_totales,
        'disponibilidad_licitada_porcentaje': round(disponibilidad_licitada_porcentaje, 1)
    }


@cache.memoize(timeout=900)  # Cache por 15 minutos
def procesar_datos_completos(df_scr, df_turnos, df_rco, fecha_inicio, fecha_fin, minera_nombre=None):
    """
    Procesar y combinar datos de SCR, Turnos y RCO (replicando lógica del notebook)
    NOTA: Función cacheada para evitar reprocesamiento costoso
    
    Args:
        df_scr: DataFrame con datos SCR
        df_turnos: DataFrame con datos de turnos
        df_rco: DataFrame con datos RCO
        fecha_inicio: Fecha inicial del rango
        fecha_fin: Fecha final del rango
        minera_nombre: Nombre de la minera (opcional, para obtener flota licitada)
    """
    print(f"🔄 [CACHE MISS] Procesando datos completos para {minera_nombre}")
    # Obtener vehículos únicos del SCR - filtrar solo valores numéricos válidos
    # OPTIMIZACIÓN: Usar operaciones vectorizadas en lugar de loops
    vehiculos_totales = df_scr['vehiclereal'].dropna()
    
    # Convertir a numérico de una vez (más rápido que loop)
    vehiculos_numericos = pd.to_numeric(vehiculos_totales, errors='coerce')
    vehiculos_validos = vehiculos_numericos[vehiculos_numericos > 0].astype(int).unique()
    vehiculos_totales = np.sort(vehiculos_validos)
    
    # Obtener vehículos licitados desde la tabla de flota o usar lista por defecto
    if minera_nombre:
        vehiculos_licitados = obtener_vehiculos_licitados_por_minera(minera_nombre)
    else:
        # Fallback a lista hardcodeada
        vehiculos_licitados = [4002, 4003, 4049, 8054, 8120, 8348, 8820]
    
    # Crear tabla base combinando todos los vehículos con todas las fechas
    # OPTIMIZACIÓN: Usar producto cartesiano de pandas en lugar de loops
    fechas_rango = pd.date_range(start=fecha_inicio, end=fecha_fin, freq='D')
    
    # Crear MultiIndex y convertir a DataFrame (más eficiente)
    import itertools
    indices = list(itertools.product(vehiculos_totales, fechas_rango))
    
    tabla_base = pd.DataFrame(indices, columns=['Camion', 'fecha_completa'])
    tabla_base['Fecha'] = tabla_base['fecha_completa'].dt.strftime('%d-%b')
    tabla_base['fecha_para_merge'] = tabla_base['fecha_completa'].dt.date
    tabla_base['Camion'] = tabla_base['Camion'].astype(int)
    tabla_base['¿Es licitado?'] = np.where(tabla_base['Camion'].isin(vehiculos_licitados), 'Si', 'No')
    
    # Procesar turnos enviados
    if not df_turnos.empty:
        # OPTIMIZACIÓN: Usar set_index para merges más rápidos
        # Obtener estado final por turno
        estado_final = (
            df_turnos.sort_values(['id_turno_uuid', 'fecha_hora'])
                     .groupby('id_turno_uuid', as_index=False)
                     .tail(1)[['id_turno_uuid', 'estado', 'id_vehiculo', 'fecha_inicio_utc']]
        )
        
        enviados = estado_final[
            (estado_final['estado'] == 'ENVIADO') &
            (estado_final['id_vehiculo'].isin(vehiculos_totales))
        ].copy()
        
        enviados['fecha'] = pd.to_datetime(enviados['fecha_inicio_utc']).dt.date
        conteo_turnos = enviados.groupby(['id_vehiculo', 'fecha']).size().reset_index(name='Turnos_enviados')
        
        # Merge con tabla base
        tabla_base = tabla_base.merge(
            conteo_turnos,
            left_on=['Camion', 'fecha_para_merge'],
            right_on=['id_vehiculo', 'fecha'],
            how='left'
        )
        tabla_base['Turnos_enviados'] = tabla_base['Turnos_enviados'].fillna(0).astype(int)
        tabla_base = tabla_base.drop(columns=['fecha', 'id_vehiculo'], errors='ignore')
    else:
        tabla_base['Turnos_enviados'] = 0
    
    # Procesar conexiones RCO
    if not df_rco.empty:
        # Filtrar solo vehículos que están en vehiculos_totales y crear copia explícita
        df_rco_filtrado = df_rco[df_rco['codigo_tanque'].isin(vehiculos_totales)].copy()
        
        # Convertir fecha_conexion_utc a zona horaria de Santiago de Chile
        df_rco_filtrado['fecha_conexion_utc'] = pd.to_datetime(df_rco_filtrado['fecha_conexion_utc'])
        df_rco_filtrado['fecha_conexion_santiago'] = (
            df_rco_filtrado['fecha_conexion_utc']
            .dt.tz_localize('UTC')
            .dt.tz_convert('America/Santiago')
        )
        
        # Hora límite para contar conexión al día siguiente (23:30 por defecto)
        # Si la hora es mayor o igual a 23:30, contar para el día siguiente
        hora_corte = pd.Timestamp('23:30').time()
        df_rco_filtrado['fecha_rco'] = df_rco_filtrado['fecha_conexion_santiago'].apply(
            lambda x: (x + pd.Timedelta(days=1)).date()
            if x.time() >= hora_corte
            else x.date()
        )
        
        conteos_rco = (df_rco_filtrado
                      .groupby(['codigo_tanque', 'fecha_rco'])
                      .size()
                      .reset_index(name='Conexion_RCO'))
        
        # Merge con tabla base
        tabla_base = tabla_base.merge(
            conteos_rco,
            left_on=['Camion', 'fecha_para_merge'],
            right_on=['codigo_tanque', 'fecha_rco'],
            how='left'
        )
        tabla_base['Conexion_RCO'] = tabla_base['Conexion_RCO'].fillna(0).astype(int)
        tabla_base = tabla_base.drop(columns=['fecha_rco', 'codigo_tanque'], errors='ignore')
    else:
        tabla_base['Conexion_RCO'] = 0
    
    # Procesar datos SCR para estados
    df_scr['fecha_corregida'] = pd.to_datetime(df_scr['vdatu']).dt.strftime('%Y-%m-%d')
    
    # Crear conteos por vehículo, fecha y estado
    conteos_scr = (df_scr
                   .groupby(['vehiclereal', 'fecha_corregida', 'descrstatu'])
                   .size()
                   .reset_index(name='cantidad'))
    
    # Crear tabla pivot para tener estados como columnas
    pivot_scr = conteos_scr.pivot_table(
        index=['vehiclereal', 'fecha_corregida'], 
        columns='descrstatu', 
        values='cantidad', 
        fill_value=0
    ).reset_index()
    
    # Limpiar nombres de columnas y asegurar que existan todas las columnas necesarias
    pivot_scr.columns.name = None
    estados_esperados = ['En Ruta', 'Entregado totalmente', 'Planificado']
    for estado in estados_esperados:
        if estado not in pivot_scr.columns:
            pivot_scr[estado] = 0
    
    # Convertir vehiclereal a int (manejar strings con decimales)
    pivot_scr['vehiclereal'] = pd.to_numeric(pivot_scr['vehiclereal'], errors='coerce').fillna(0).astype(int)
    
    # Obtener transportista por vehículo y fecha
    carriers = (
        df_scr[['vehiclereal', 'fecha_corregida', 'carriername1']]
        .dropna(subset=['carriername1'])
        .drop_duplicates(subset=['vehiclereal', 'fecha_corregida'])
        .groupby(['vehiclereal', 'fecha_corregida'], as_index=False).first()
    )
    # Convertir vehiclereal a int (manejar strings con decimales)
    carriers['vehiclereal'] = pd.to_numeric(carriers['vehiclereal'], errors='coerce').fillna(0).astype(int)
    
    # Crear fecha para merge - usar el año de fecha_completa en lugar de hardcodear
    tabla_base['fecha_merge_scr'] = tabla_base['fecha_completa'].dt.strftime('%Y-%m-%d')
    
    # Merge con datos SCR
    tabla_final = tabla_base.merge(
        pivot_scr,
        left_on=['Camion', 'fecha_merge_scr'],
        right_on=['vehiclereal', 'fecha_corregida'],
        how='left'
    )
    
    # Rellenar NaN con 0 para los estados
    for estado in estados_esperados:
        if estado in tabla_final.columns:
            tabla_final[estado] = tabla_final[estado].fillna(0).astype(int)
    
    # Merge con transportistas
    tabla_final = tabla_final.merge(
        carriers,
        left_on=['Camion', 'fecha_merge_scr'],
        right_on=['vehiclereal', 'fecha_corregida'],
        how='left'
    )
    
    # Procesar transportista
    tabla_final['Transportista'] = tabla_final.get('carriername1', pd.Series([None]*len(tabla_final)))
    tabla_final['Transportista'] = tabla_final['Transportista'].replace('', pd.NA)
    
    # Llenar transportista faltante con el más común por vehículo
    per_truck = carriers.groupby('vehiclereal', as_index=False)['carriername1'].first().set_index('vehiclereal')['carriername1'].to_dict()
    tabla_final['Transportista'] = tabla_final['Transportista'].fillna(tabla_final['Camion'].map(per_truck))
    tabla_final['Transportista'] = tabla_final['Transportista'].fillna('SIN_INFORMACION').astype(str)
    
    # Limpiar columnas auxiliares
    tabla_final = tabla_final.drop(['fecha_merge_scr', 'vehiclereal', 'fecha_corregida', 'carriername1', 'fecha_para_merge'], axis=1, errors='ignore')
    
    # Renombrar columnas para mayor claridad
    tabla_final = tabla_final.rename(columns={
        'En Ruta': 'En_ruta',
        'Entregado totalmente': 'Entregado_totalmente',
        'Turnos_enviados': 'Turnos_enviados'
    })
    
    # Filtrar transportistas sin información
    tabla_final = tabla_final[tabla_final['Transportista'] != 'SIN_INFORMACION']
    
    return tabla_final


# Funciones para obtener datos desde Athena
@cache.memoize(timeout=900)  # Cache por 15 minutos (datos cambian poco)
def obtener_datos_desde_athena(minera_nombre, fecha_inicio, fecha_fin):
    """
    Obtener datos desde Athena siguiendo la lógica de athena_test.py
    NOTA: Esta función está cacheada por 15 minutos para reducir queries costosas
    """
    print(f"🔄 [CACHE MISS] Ejecutando query SCR para {minera_nombre} ({fecha_inicio} - {fecha_fin})")
    
    if not USE_ATHENA:
        return None

    # Manejar el caso especial de CODELCO
    if minera_nombre == 'CODELCO':
        mineras_codelco = obtener_mineras_codelco()
        # Crear query para todas las mineras de CODELCO
        conditions = []
        for minera in mineras_codelco:
            minera_safe = minera.replace("'", "''")
            conditions.append(f"vtext LIKE '{minera_safe}'")
        
        # OPTIMIZACIÓN: Seleccionar solo columnas necesarias
        query = f"""
        SELECT 
            vdatu,
            vtext,
            vehiclereal,
            carriername1,
            descrstatu
        FROM {DATABASE_ATHENA}.{TABLA_PEDIDOS}
        WHERE vdatu >= '{fecha_inicio.strftime('%Y-%m-%d')}'
          AND vdatu <= '{fecha_fin.strftime('%Y-%m-%d')}'
          AND ({' OR '.join(conditions)})
          AND carriername1 IS NOT NULL
          AND carriername1 != 'SIN_INFORMACION'
        """
    else:
        # Caso normal para mineras individuales
        minera_safe = minera_nombre.replace("'", "''")
        # OPTIMIZACIÓN: Seleccionar solo columnas necesarias
        query = f"""
        SELECT 
            vdatu,
            vtext,
            vehiclereal,
            carriername1,
            descrstatu
        FROM {DATABASE_ATHENA}.{TABLA_PEDIDOS}
        WHERE vdatu >= '{fecha_inicio.strftime('%Y-%m-%d')}'
          AND vdatu <= '{fecha_fin.strftime('%Y-%m-%d')}'
          AND vtext LIKE '{minera_safe}'
          AND carriername1 IS NOT NULL
          AND carriername1 != 'SIN_INFORMACION'
        """

    try:
        df = sql_athena(query)
        if df.empty:
            return None

        # Procesar datos según lógica de athena_test.py
        df['Fecha'] = pd.to_datetime(df['vdatu'], format='%Y-%m-%d', errors='coerce')

        # Calcular entregas (descrstatu) - igual que en athena_test.py
        df['Entregado totalmente'] = (
            (df['descrstatu'] == 'Entregado totalmente') |
            (df['descrstatu'] == 'Recibí Conforme')
        ).astype(int)

        # Limpiar transportista
        df['Transportista'] = df['carriername1'].fillna('SIN_INFORMACION')
        
        # Excluir registros con transportista 'SIN_INFORMACION'
        df = df[df['Transportista'] != 'SIN_INFORMACION']
        
        # Verificar si quedaron datos después del filtro
        if df.empty:
            return None

        return df

    except Exception as e:
        print(f"Error obteniendo datos de Athena: {e}")
        return None


def procesar_datos_athena(df, minimo_viajes, maximo_viajes):
    """
    Procesar DataFrame de Athena según lógica de athena_test.py
    """
    # Crear tabla base - agregación por Fecha y Transportista
    tabla_base = df.groupby(['Fecha', 'Transportista']).agg({
        'Entregado totalmente': 'sum'
    }).reset_index()

    # Resumen diario
    resumen_diario = tabla_base.groupby('Fecha').agg({
        'Entregado totalmente': 'sum'
    }).reset_index()

    # Verificar si están en rango
    resumen_diario['En_rango'] = (
        (resumen_diario['Entregado totalmente'] >= minimo_viajes) &
        (resumen_diario['Entregado totalmente'] <= maximo_viajes)
    )

    return tabla_base, resumen_diario


@cache.cached(timeout=3600, key_prefix='mineras_athena')  # Cache por 1 hora
def obtener_mineras_athena():
    """Obtener lista de mineras predefinidas"""
    # Lista fija de mineras requeridas
    mineras_predefinidas = [
        'MINA LA ESCONDIDA',
        'QUADRA SIERRA GORDA', 
        'ANDINA',
        'EL TENIENTE',
        'CASERONES',
        'SALARES NORTE',
        'MINERA CANDELARIA',
        'LOS BRONCES',
        'CODELCO'  # Agrupa: MINISTRO HALES, RADOMIRO TOMIC, CHUQUICAMATA, MINA GABY
    ]
    return mineras_predefinidas


@cache.cached(timeout=3600, key_prefix='mineras_codelco')  # Cache por 1 hora
def obtener_mineras_codelco():
    """Obtener lista de mineras que forman parte de CODELCO"""
    return ['MINISTRO HALES', 'RADOMIRO TOMIC', 'CHUQUICAMATA', 'MINA GABY']


def es_minera_codelco(minera_nombre):
    """Verificar si una minera pertenece al grupo CODELCO"""
    return minera_nombre in obtener_mineras_codelco()


def obtener_mapeo_uso_mineras():
    """Obtener mapeo entre valores de 'uso' en tabla flota y nombres de mineras"""
    mapeo_uso = {
        # Codelco (agrupación)
        'RADOMIRO TOMIC': 'CODELCO',
        'MINISTRO HALES': 'CODELCO',
        'CODELCO CHUQUI': 'CODELCO',
        'CODELCO CHUQUI PC': 'CODELCO',
        'APOYO CODELCO': 'CODELCO',
        
        # Mineras individuales
        'CASERONES': 'CASERONES',
        'CODELCO TENIENTE': 'EL TENIENTE',
        'LOS BRONCES': 'LOS BRONCES',
        'CODELCO ANDINA': 'ANDINA',
        'QUADRA': 'QUADRA SIERRA GORDA',
        'SALARES NORTE': 'SALARES NORTE',
        'CANDELARIA': 'MINERA CANDELARIA'
    }
    return mapeo_uso


@cache.memoize(timeout=600)  # Cache por 10 minutos con parámetros
def obtener_flota_licitada_athena(minera_nombre):
    """
    Obtener vehículos de flota licitada desde Excel local o Athena (fallback)
    
    Prioridad:
    1. Excel local (Flota 20260108.xlsx) - NO se sube a Git
    2. Athena (fallback si no existe el Excel)
    
    Args:
        minera_nombre: Nombre de la minera (según nomenclatura de la app)
    
    Returns:
        DataFrame con columnas: codigo_tanque, nombre_transportista, uso
        o DataFrame vacío si no hay datos o hay error
    """
    # PRIORIDAD 1: Intentar leer desde archivo TXT tabular
    # Buscar en múltiples ubicaciones (Render Secret Files + local)
    rutas_txt_posibles = [
        '/etc/secrets/mantenedor_flota.txt',  # Render Secret Files (ubicación alternativa)
        os.path.join(os.path.dirname(__file__), 'mantenedor_flota.txt'),  # Raíz de la app (Render o local)
        os.path.join(os.path.dirname(__file__), 'Flota_20260108 - copia.txt')  # Nombre alternativo local
    ]
    
    ruta_txt = None
    for ruta in rutas_txt_posibles:
        if os.path.exists(ruta):
            ruta_txt = ruta
            break
    
    if ruta_txt:
        try:
            print(f"\n{'='*80}")
            print(f"📄 LEYENDO FLOTA DESDE TXT LOCAL")
            print(f"   Ruta: {ruta_txt}")
            print(f"   Minera solicitada: {minera_nombre}")
            
            # Intentar múltiples codificaciones
            encodings = ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']
            df_txt = None
            encoding_used = None
            
            for encoding in encodings:
                try:
                    df_txt = pd.read_csv(ruta_txt, sep='\t', encoding=encoding)
                    encoding_used = encoding
                    print(f"   ✓ TXT cargado exitosamente con codificación: {encoding}")
                    break
                except (UnicodeDecodeError, UnicodeError):
                    continue
            
            if df_txt is None:
                raise Exception(f"No se pudo decodificar el archivo con ninguna codificación: {encodings}")
            
            print(f"   ✓ Total de registros en TXT: {len(df_txt)}")
            print(f"   ✓ Columnas encontradas: {list(df_txt.columns)}")
            
            # Validar columnas requeridas
            columnas_requeridas = ['Equipo', 'Transportista', 'Uso']
            if all(col in df_txt.columns for col in columnas_requeridas):
                # Renombrar columnas al formato esperado por la aplicación
                df_txt = df_txt.rename(columns={
                    'Equipo': 'codigo_tanque',
                    'Transportista': 'nombre_transportista',
                    'Uso': 'uso'
                })
                
                # Obtener mapeo de uso a mineras
                mapeo_uso = obtener_mapeo_uso_mineras()
                
                # Invertir el mapeo para buscar por minera
                uso_values = [uso for uso, minera in mapeo_uso.items() if minera == minera_nombre]
                print(f"   ✓ Valores de 'Uso' buscados para {minera_nombre}: {uso_values}")
                
                if not uso_values:
                    print(f"   ⚠️ No se encontró mapeo de uso para minera: {minera_nombre}")
                    print(f"{'='*80}\n")
                    return pd.DataFrame()
                
                # Mostrar valores únicos de 'uso' en el TXT para debug
                print(f"   ✓ Valores únicos de 'Uso' en TXT: {df_txt['uso'].unique().tolist()}")
                
                # Filtrar por minera (todos están activos, no hay columna 'estado')
                df_flota = df_txt[df_txt['uso'].isin(uso_values)].copy()
                print(f"   ✓ Vehículos encontrados después de filtrar por uso: {len(df_flota)}")
                
                if df_flota.empty:
                    print(f"   ⚠️ No se encontraron vehículos para {minera_nombre} en TXT")
                    print(f"{'='*80}\n")
                    return pd.DataFrame()
                
                # Convertir codigo_tanque a int
                df_flota['codigo_tanque'] = df_flota['codigo_tanque'].astype(int)
                print(f"   ✓ Códigos de tanque convertidos a enteros")
                
                # Eliminar duplicados
                df_flota = df_flota.drop_duplicates(subset=['codigo_tanque'], keep='first')
                print(f"   ✓ Duplicados eliminados (si existían)")
                
                # Mostrar primeros vehículos como muestra
                print(f"   ✓ Muestra de vehículos cargados:")
                for idx, row in df_flota.head(3).iterrows():
                    print(f"      - Equipo {row['codigo_tanque']}: {row['nombre_transportista']} (Uso: {row['uso']})")
                
                print(f"   ✅ FLOTA CARGADA EXITOSAMENTE DESDE TXT")
                print(f"   ✅ Total de vehículos: {len(df_flota)} para {minera_nombre}")
                print(f"{'='*80}\n")
                return df_flota
            else:
                print(f"   ❌ TXT no tiene las columnas requeridas: {columnas_requeridas}")
                print(f"   ❌ Columnas encontradas: {list(df_txt.columns)}")
                print(f"{'='*80}\n")
        except Exception as e:
            print(f"   ❌ Error leyendo TXT local: {e}")
            print(f"   ↪️  Intentando con Excel como fallback...")
            print(f"{'='*80}\n")
    else:
        print(f"\n{'='*80}")
        print(f"📁 TXT NO ENCONTRADO en ninguna ubicación")
        print(f"   Ubicaciones buscadas:")
        for ruta in rutas_txt_posibles:
            print(f"   - {ruta}")
        print(f"↪️  Intentando con Excel como fallback...")
        print(f"{'='*80}\n")
    
    # PRIORIDAD 2: Intentar leer desde Excel local
    ruta_excel = os.path.join(os.path.dirname(__file__), 'Flota_20260108.xlsx')
    
    if os.path.exists(ruta_excel):
        try:
            print(f"\n{'='*80}")
            print(f"📊 LEYENDO FLOTA DESDE EXCEL LOCAL")
            print(f"   Ruta: {ruta_excel}")
            print(f"   Minera solicitada: {minera_nombre}")
            df_excel = pd.read_excel(ruta_excel)
            print(f"   ✓ Excel cargado exitosamente")
            print(f"   ✓ Total de registros en Excel: {len(df_excel)}")
            print(f"   ✓ Columnas encontradas: {list(df_excel.columns)}")
            
            # Validar columnas requeridas (según estructura del nuevo Excel)
            columnas_requeridas = ['Equipo', 'Transportista', 'Uso']
            if all(col in df_excel.columns for col in columnas_requeridas):
                # Renombrar columnas al formato esperado por la aplicación
                df_excel = df_excel.rename(columns={
                    'Equipo': 'codigo_tanque',
                    'Transportista': 'nombre_transportista',
                    'Uso': 'uso'
                })
                
                # Obtener mapeo de uso a mineras
                mapeo_uso = obtener_mapeo_uso_mineras()
                
                # Invertir el mapeo para buscar por minera
                uso_values = [uso for uso, minera in mapeo_uso.items() if minera == minera_nombre]
                print(f"   ✓ Valores de 'Uso' buscados para {minera_nombre}: {uso_values}")
                
                if not uso_values:
                    print(f"   ⚠️ No se encontró mapeo de uso para minera: {minera_nombre}")
                    print(f"{'='*80}\n")
                    return pd.DataFrame()
                
                # Mostrar valores únicos de 'uso' en el Excel para debug
                print(f"   ✓ Valores únicos de 'Uso' en Excel: {df_excel['uso'].unique().tolist()}")
                
                # Filtrar por minera (todos están activos, no hay columna 'estado')
                df_flota = df_excel[df_excel['uso'].isin(uso_values)].copy()
                print(f"   ✓ Vehículos encontrados después de filtrar por uso: {len(df_flota)}")
                
                if df_flota.empty:
                    print(f"   ⚠️ No se encontraron vehículos para {minera_nombre} en Excel")
                    print(f"{'='*80}\n")
                    return pd.DataFrame()
                
                # Convertir codigo_tanque a int
                df_flota['codigo_tanque'] = df_flota['codigo_tanque'].astype(int)
                print(f"   ✓ Códigos de tanque convertidos a enteros")
                
                # Eliminar duplicados
                df_flota = df_flota.drop_duplicates(subset=['codigo_tanque'], keep='first')
                print(f"   ✓ Duplicados eliminados (si existían)")
                
                # Mostrar primeros vehículos como muestra
                print(f"   ✓ Muestra de vehículos cargados:")
                for idx, row in df_flota.head(3).iterrows():
                    print(f"      - Equipo {row['codigo_tanque']}: {row['nombre_transportista']} (Uso: {row['uso']})")
                
                print(f"   ✅ FLOTA CARGADA EXITOSAMENTE DESDE EXCEL")
                print(f"   ✅ Total de vehículos: {len(df_flota)} para {minera_nombre}")
                print(f"{'='*80}\n")
                return df_flota
            else:
                print(f"   ❌ Excel no tiene las columnas requeridas: {columnas_requeridas}")
                print(f"   ❌ Columnas encontradas: {list(df_excel.columns)}")
                print(f"{'='*80}\n")
        except Exception as e:
            print(f"   ❌ Error leyendo Excel local: {e}")
            print(f"   ↪️  Intentando con Athena como fallback...")
            print(f"{'='*80}\n")
    else:
        print(f"\n{'='*80}")
        print(f"📁 Excel local NO ENCONTRADO: {ruta_excel}")
        print(f"↪️  Usando Athena como fallback...")
        print(f"{'='*80}\n")
    
    # PRIORIDAD 3: Fallback a Athena si no hay TXT/Excel o hay error
    if not USE_ATHENA:
        return pd.DataFrame()
    
    # Validar que existe la tabla de flota configurada
    if not TABLA_FLOTA:
        print("TABLA_FLOTA no está configurada en las variables de entorno")
        return pd.DataFrame()
    
    # Obtener mapeo de uso a mineras
    mapeo_uso = obtener_mapeo_uso_mineras()
    
    # Invertir el mapeo para buscar por minera
    uso_values = [uso for uso, minera in mapeo_uso.items() if minera == minera_nombre]
    
    if not uso_values:
        print(f"No se encontró mapeo de uso para minera: {minera_nombre}")
        return pd.DataFrame()
    
    # Crear condiciones para la query con escape correcto
    uso_conditions = [f"'{uso}'" for uso in uso_values]
    uso_in_clause = ', '.join(uso_conditions)
    
    query = f"""
    SELECT 
        codigo_tanque,
        nombre_transportista,
        estado,
        uso
    FROM {DATABASE_DISPOMATE}.{TABLA_FLOTA}
    WHERE uso IN ({uso_in_clause})
      AND estado = 'Activo'
    ORDER BY codigo_tanque
    """
    
    try:
        df_flota = sql_athena(query)
        if df_flota.empty:
            print(f"No se encontraron vehículos licitados para {minera_nombre} en Athena")
            return pd.DataFrame()
        
        # Convertir codigo_tanque a int
        df_flota['codigo_tanque'] = df_flota['codigo_tanque'].astype(int)
        
        # Eliminar duplicados - mantener solo un registro por vehículo
        # Priorizar el primer registro encontrado
        df_flota = df_flota.drop_duplicates(subset=['codigo_tanque'], keep='first')
        
        print(f"✅ Flota cargada desde Athena: {len(df_flota)} vehículos para {minera_nombre}")
        return df_flota
    except Exception as e:
        print(f"Error obteniendo flota licitada de Athena: {e}")
        return pd.DataFrame()


def obtener_vehiculos_licitados_por_minera(minera_nombre):
    """
    Obtener lista de códigos de vehículos licitados para una minera
    
    Args:
        minera_nombre: Nombre de la minera
    
    Returns:
        Lista de códigos de vehículos (int)
    """
    df_flota = obtener_flota_licitada_athena(minera_nombre)
    
    if df_flota.empty:
        # Fallback a lista hardcodeada (mantener compatibilidad)
        return [4002, 4003, 4049, 8054, 8120, 8348, 8820]
    
    return sorted(df_flota['codigo_tanque'].unique().tolist())


def obtener_actividad_flota_licitada(minera_nombre, fecha_inicio, fecha_fin):
    """
    Obtener actividad detallada de cada vehículo de la flota licitada
    
    Args:
        minera_nombre: Nombre de la minera
        fecha_inicio: Fecha inicial del rango
        fecha_fin: Fecha final del rango
    
    Returns:
        Lista de diccionarios con actividad de cada vehículo:
        - codigo_tanque: ID del vehículo
        - nombre_transportista: Transportista del vehículo
        - estado: Estado del vehículo en flota
        - uso: Uso asignado
        - turnos_enviados: Total de turnos enviados
        - conexiones_rco: Total de conexiones RCO
        - entregas_realizadas: Total de entregas completadas
        - destinos: Lista de destinos únicos de entregas (vtext)
        - detalle_entregas: Lista de entregas con destino y estado
    """
    if not USE_ATHENA:
        return []
    
    # 1. Obtener flota licitada
    df_flota = obtener_flota_licitada_athena(minera_nombre)
    if df_flota.empty:
        return []
    
    vehiculos_licitados = df_flota['codigo_tanque'].tolist()
    
    # 2. Obtener turnos enviados para estos vehículos
    query_turnos = f"""
    SELECT id_vehiculo, COUNT(DISTINCT id_turno_uuid) as turnos_enviados
    FROM (
        SELECT id_turno_uuid, id_vehiculo, estado,
               ROW_NUMBER() OVER (PARTITION BY id_turno_uuid ORDER BY fecha_hora DESC) as rn
        FROM {DATABASE_DISPOMATE}.{TABLA_TURNOS}
        WHERE DATE(fecha_inicio_utc) >= DATE('{fecha_inicio.strftime('%Y-%m-%d')}')
          AND DATE(fecha_inicio_utc) <= DATE('{fecha_fin.strftime('%Y-%m-%d')}')
          AND id_vehiculo IN ({','.join(map(str, vehiculos_licitados))})
    )
    WHERE rn = 1 AND estado = 'ENVIADO'
    GROUP BY id_vehiculo
    """
    
    try:
        df_turnos = sql_athena(query_turnos)
    except Exception as e:
        print(f"Error obteniendo turnos de flota licitada: {e}")
        df_turnos = pd.DataFrame()
    
    # 3. Obtener conexiones RCO para estos vehículos
    query_rco = f"""
    SELECT codigo_tanque, COUNT(*) as conexiones_rco
    FROM {DATABASE_DISPOMATE}.{TABLA_RCO}
    WHERE DATE(fecha_conexion_utc) >= DATE('{fecha_inicio.strftime('%Y-%m-%d')}')
      AND DATE(fecha_conexion_utc) <= DATE('{fecha_fin.strftime('%Y-%m-%d')}')
      AND codigo_tanque IN ({','.join(map(str, vehiculos_licitados))})
    GROUP BY codigo_tanque
    """
    
    try:
        df_rco = sql_athena(query_rco)
    except Exception as e:
        print(f"Error obteniendo conexiones RCO de flota licitada: {e}")
        df_rco = pd.DataFrame()
    
    # 4. Obtener entregas/pedidos con destinos para estos vehículos
    query_entregas = f"""
    SELECT 
        CAST(vehiclereal AS INTEGER) as vehiclereal,
        vtext as destino,
        descrstatu as estado,
        COUNT(*) as cantidad
    FROM {DATABASE_ATHENA}.{TABLA_PEDIDOS}
    WHERE vdatu >= '{fecha_inicio.strftime('%Y-%m-%d')}'
      AND vdatu <= '{fecha_fin.strftime('%Y-%m-%d')}'
      AND CAST(vehiclereal AS INTEGER) IN ({','.join(map(str, vehiculos_licitados))})
    GROUP BY CAST(vehiclereal AS INTEGER), vtext, descrstatu
    """
    
    try:
        df_entregas = sql_athena(query_entregas)
        if not df_entregas.empty:
            df_entregas['vehiclereal'] = df_entregas['vehiclereal'].astype(int)
    except Exception as e:
        print(f"Error obteniendo entregas de flota licitada: {e}")
        df_entregas = pd.DataFrame()
    
    # 5. Combinar toda la información
    actividad_flota = []
    
    for _, vehiculo in df_flota.iterrows():
        codigo_tanque = int(vehiculo['codigo_tanque'])
        
        # Turnos enviados
        turnos = 0
        if not df_turnos.empty and 'id_vehiculo' in df_turnos.columns:
            turno_row = df_turnos[df_turnos['id_vehiculo'] == codigo_tanque]
            if not turno_row.empty:
                turnos = int(turno_row['turnos_enviados'].iloc[0])
        
        # Conexiones RCO
        conexiones = 0
        if not df_rco.empty and 'codigo_tanque' in df_rco.columns:
            rco_row = df_rco[df_rco['codigo_tanque'] == codigo_tanque]
            if not rco_row.empty:
                conexiones = int(rco_row['conexiones_rco'].iloc[0])
        
        # Entregas y destinos
        entregas_totales = 0
        destinos = []
        detalle_entregas = []
        
        if not df_entregas.empty:
            entregas_vehiculo = df_entregas[df_entregas['vehiclereal'] == codigo_tanque]
            
            if not entregas_vehiculo.empty:
                # Contar entregas completadas
                entregas_completadas = entregas_vehiculo[
                    entregas_vehiculo['estado'].isin(['Entregado totalmente', 'Recibí Conforme'])
                ]
                entregas_totales = int(entregas_completadas['cantidad'].sum()) if not entregas_completadas.empty else 0
                
                # Obtener destinos únicos
                destinos = entregas_vehiculo['destino'].unique().tolist()
                
                # Detalle de entregas por destino y estado
                for _, entrega in entregas_vehiculo.iterrows():
                    detalle_entregas.append({
                        'destino': entrega['destino'],
                        'estado': entrega['estado'],
                        'cantidad': int(entrega['cantidad'])
                    })
        
        actividad_flota.append({
            'codigo_tanque': codigo_tanque,
            'nombre_transportista': vehiculo['nombre_transportista'],
            'estado': 'Activo',  # Todos están activos en el Excel nuevo
            'uso': vehiculo['uso'],
            'turnos_enviados': turnos,
            'conexiones_rco': conexiones,
            'entregas_realizadas': entregas_totales,
            'destinos': destinos,
            'destinos_str': ', '.join(destinos) if destinos else 'Sin entregas',
            'detalle_entregas': detalle_entregas,
            'tiene_actividad': turnos > 0 or conexiones > 0 or entregas_totales > 0
        })
    
    return actividad_flota


@cache.cached(timeout=3600, key_prefix='config_viajes')  # Cache por 1 hora
def obtener_configuracion_viajes():
    """Obtener configuración de bandas mínimas y máximas de viajes por minera"""
    configuracion_viajes = {
        'MINA LA ESCONDIDA': {'minimo': 38, 'maximo': 38},
        'QUADRA SIERRA GORDA': {'minimo': 11, 'maximo': 13}, 
        'ANDINA': {'minimo': 6, 'maximo': 7},
        'EL TENIENTE': {'minimo': 3, 'maximo': 4},
        'CASERONES': {'minimo': 5, 'maximo': 6},
        'SALARES NORTE': {'minimo': 4, 'maximo': 5},
        'MINERA CANDELARIA': {'minimo': 9, 'maximo': 9},
        'LOS BRONCES': {'minimo': 12, 'maximo': 12},
        'CODELCO': {'minimo': 36, 'maximo': 42}  
    }
    return configuracion_viajes


@cache.cached(timeout=3600, key_prefix='config_bandas')  # Cache por 1 hora
def obtener_configuracion_bandas_transportista():
    """Obtener configuración de bandas de capacidad por transportista"""
    # Banda estándar por transportista (viajes por día)
    bandas_transportista = {
        # Configuraciones específicas por transportista y minera
        'SOC. DE TRANSP. ILZAUSPE LTDA.': {
            'MINA LA ESCONDIDA': 13,
            'QUADRA SIERRA GORDA': 11,
            'ANDINA': 6,
            'LOS BRONCES': 6
        },
        'TRANSPORTES DE COMBUSTIBLES CHILE L': {
            'MINA LA ESCONDIDA': 14,
            'SALARES NORTE': 4,
            'MINERA CANDELARIA': 9
        },
        'TRANSPORTES SOLUCIONES LOGISTICAS': {
            'MINA LA ESCONDIDA': 11,
            'CODELCO': 11
        },
        'SCP SOTRASER S.A.': {
            'CODELCO': 12
        },
        'TRANSPORTES VIGAL S.A.': {
            'EL TENIENTE': 3,
            'CASERONES': 5,
            'CODELCO': 12
        },
        'SOCIEDAD DE TRANSPORTE NAZAR LTDA': {
            'LOS BRONCES': 6
        }
    }
    return bandas_transportista


def obtener_transportistas_autorizados(minera):
    """Obtener transportistas autorizados para una minera específica"""
    bandas = obtener_configuracion_bandas_transportista()
    transportistas_autorizados = set()
    
    for transportista, mineras_config in bandas.items():
        if minera in mineras_config:
            transportistas_autorizados.add(transportista)
    
    return transportistas_autorizados


def es_transportista_autorizado(transportista, minera):
    """Verificar si un transportista está autorizado para una minera"""
    transportistas_autorizados = obtener_transportistas_autorizados(minera)
    
    # Normalizar nombre del transportista para búsqueda
    transportista_normalizado = transportista.upper().strip()
    
    # Buscar exacto
    if transportista_normalizado in [t.upper() for t in transportistas_autorizados]:
        return True
    
    # Buscar por patrones parciales
    for nombre_autorizado in transportistas_autorizados:
        if (nombre_autorizado.upper() in transportista_normalizado or 
            transportista_normalizado in nombre_autorizado.upper()):
            return True
    
    return False


def calcular_banda_transportista(transportista, minera, bandas_config=None):
    """Calcular la banda (capacidad diaria) de un transportista para una minera específica"""
    if bandas_config is None:
        bandas_config = obtener_configuracion_bandas_transportista()
    
    # Verificar si es transportista autorizado
    if not es_transportista_autorizado(transportista, minera):
        return None  # No tiene banda, se medirá por entregas
    
    # Normalizar nombre del transportista para búsqueda
    transportista_normalizado = transportista.upper().strip()
    
    # Buscar configuración específica del transportista (exacta)
    for nombre_config, mineras_config in bandas_config.items():
        if minera in mineras_config:
            if (transportista_normalizado == nombre_config.upper() or
                nombre_config.upper() in transportista_normalizado or 
                transportista_normalizado in nombre_config.upper()):
                return mineras_config[minera]
    
    # Si llegamos aquí, es un error en la configuración
    return None


def obtener_transportistas_athena(minera_nombre, fecha_inicio, fecha_fin):
    """Obtener transportistas únicos para una minera y rango de fechas"""
    df = obtener_datos_desde_athena(minera_nombre, fecha_inicio, fecha_fin)
    if df is None or df.empty:
        return []
    # Ya filtrado en obtener_datos_desde_athena, pero por seguridad adicional
    transportistas = sorted(df['Transportista'].unique().tolist())
    return [t for t in transportistas if t != 'SIN_INFORMACION']


@cache.memoize(timeout=900)  # Cache por 15 minutos
def obtener_datos_grafico_athena(minera_nombre, fecha_inicio, fecha_fin, viajes_min, viajes_max):
    """Obtener datos para gráfico con cache"""
    print(f"🔄 [CACHE MISS] Generando datos de gráfico para {minera_nombre}")
    df = obtener_datos_desde_athena(minera_nombre, fecha_inicio, fecha_fin)
    if df is None or df.empty:
        return {'datos': [], 'minimo': viajes_min, 'maximo': viajes_max, 'tipo': 'apilado', 'transportistas': []}

    tabla_base, resumen_diario = procesar_datos_athena(df, viajes_min, viajes_max)

    # Separar transportistas autorizados y otros
    transportistas_unicos = tabla_base['Transportista'].unique()
    transportistas_autorizados = []
    otros_transportistas = []
    
    for transportista in transportistas_unicos:
        if es_transportista_autorizado(transportista, minera_nombre):
            transportistas_autorizados.append(transportista)
        else:
            otros_transportistas.append(transportista)
    
    # Lista final para el gráfico
    transportistas_finales = sorted(transportistas_autorizados)
    if otros_transportistas:
        transportistas_finales.append('OTRO TRANSPORTISTA')
    
    # Crear estructura para gráfico apilado
    fechas_ordenadas = sorted(tabla_base['Fecha'].unique())
    
    datos_apilados = []
    for fecha in fechas_ordenadas:
        fecha_str = fecha.strftime('%d-%m')
        datos_fecha = {'fecha': fecha_str, 'total': 0}
        
        # Agregar datos por transportista autorizado
        for transportista in transportistas_autorizados:
            datos_transportista = tabla_base[
                (tabla_base['Fecha'] == fecha) & 
                (tabla_base['Transportista'] == transportista)
            ]
            cantidad = int(datos_transportista['Entregado totalmente'].sum()) if not datos_transportista.empty else 0
            datos_fecha[transportista] = cantidad
            datos_fecha['total'] += cantidad
        
        # Agregar datos agrupados de "otros" transportistas
        if otros_transportistas:
            cantidad_otros = 0
            for transportista in otros_transportistas:
                datos_transportista = tabla_base[
                    (tabla_base['Fecha'] == fecha) & 
                    (tabla_base['Transportista'] == transportista)
                ]
                cantidad_otros += int(datos_transportista['Entregado totalmente'].sum()) if not datos_transportista.empty else 0
            
            datos_fecha['OTRO TRANSPORTISTA'] = cantidad_otros
            datos_fecha['total'] += cantidad_otros
        
        # Verificar si cumple con el rango
        datos_fecha['cumple'] = viajes_min <= datos_fecha['total'] <= viajes_max
        datos_apilados.append(datos_fecha)

    grafico_data = {
        'datos': datos_apilados,
        'minimo': viajes_min,
        'maximo': viajes_max,
        'tipo': 'apilado',
        'transportistas': transportistas_finales
    }

    return grafico_data


@cache.memoize(timeout=900)  # Cache por 15 minutos
def obtener_datos_matriz_athena(minera_nombre, fecha_inicio, fecha_fin, viajes_min, viajes_max):
    """Obtener datos para matriz con cache"""
    print(f"🔄 [CACHE MISS] Generando matriz de datos para {minera_nombre}")
    df = obtener_datos_desde_athena(minera_nombre, fecha_inicio, fecha_fin)
    if df is None or df.empty:
        return {'transportistas': [], 'fechas': [], 'datos': {}}

    tabla_base, resumen_diario = procesar_datos_athena(df, viajes_min, viajes_max)

    # Obtener datos completos para cálculos de disponibilidad operacional
    datos_completos = obtener_datos_completos_athena(minera_nombre, fecha_inicio, fecha_fin)
    
    # Separar transportistas autorizados y otros
    transportistas_unicos = tabla_base['Transportista'].unique()
    transportistas_autorizados = []
    otros_transportistas_data = tabla_base[tabla_base['Transportista'].isin([])].copy()  # Inicializar vacío
    
    for transportista in transportistas_unicos:
        if es_transportista_autorizado(transportista, minera_nombre):
            transportistas_autorizados.append(transportista)
        else:
            # Agregar a "otros" transportistas
            datos_transportista = tabla_base[tabla_base['Transportista'] == transportista]
            otros_transportistas_data = pd.concat([otros_transportistas_data, datos_transportista], ignore_index=True)
    
    # Lista final de transportistas para mostrar
    transportistas_finales = transportistas_autorizados.copy()
    if not otros_transportistas_data.empty:
        transportistas_finales.append('OTRO TRANSPORTISTA')
    
    fechas = [d.strftime('%d-%m') for d in sorted(resumen_diario['Fecha'].unique())]

    matriz_data = {
        'transportistas': transportistas_finales,
        'fechas': fechas,
        'datos': {},
        'bandas_transportistas': {}  # Para referencia en el frontend
    }

    # Procesar transportistas autorizados
    for transportista_nombre in transportistas_autorizados:
        banda_transportista = calcular_banda_transportista(transportista_nombre, minera_nombre)
        matriz_data['bandas_transportistas'][transportista_nombre] = banda_transportista
        
        matriz_data['datos'][transportista_nombre] = {}
        for _, row in resumen_diario.iterrows():
            fecha_str = row['Fecha'].strftime('%d-%m')
            fecha_actual = row['Fecha'].date()
            datos_trans = tabla_base[(tabla_base['Transportista'] == transportista_nombre) & (tabla_base['Fecha'] == row['Fecha'])]
            viajes_realizados = int(datos_trans['Entregado totalmente'].sum())
            
            # Calcular disponibilidad por banda: (viajes realizados / banda transportista) * 100
            if banda_transportista and banda_transportista > 0:
                disponibilidad_banda = (viajes_realizados / banda_transportista) * 100
                disponibilidad_banda = min(100, disponibilidad_banda)
            else:
                disponibilidad_banda = 0
            
            # Calcular disponibilidad operacional para este transportista y fecha
            # Usar la misma función que usa el detalle para consistencia
            disp_op_data = {'porcentaje': 0, 'turnos_enviados': 0, 'entregas_exitosas': 0}
            entregas_licitadas = 0
            entregas_totales = 0
            
            if datos_completos is not None and not datos_completos.empty:
                # Convertir la columna Fecha a datetime para comparación correcta
                # La columna 'Fecha' en datos_completos tiene formato '22-Nov' (string)
                # Necesitamos comparar con fecha_actual que es date
                fecha_str_buscar = fecha_actual.strftime('%d-%b')  # Ejemplo: '22-Nov'
                
                # Filtrar datos completos por transportista y fecha
                datos_dia = datos_completos[
                    (datos_completos['Transportista'] == transportista_nombre) &
                    (datos_completos['Fecha'] == fecha_str_buscar)
                ]
                
                if not datos_dia.empty:
                    # IMPORTANTE: Filtrar solo camiones que tienen actividad SCR ese día
                    # Solo incluir camiones que tienen entregas, en ruta o planificado > 0
                    # Excluir camiones que SOLO tienen turnos/RCO pero no actividad SCR
                    datos_dia_con_actividad = datos_dia[
                        (datos_dia['Entregado_totalmente'] > 0) |
                        (datos_dia.get('En_ruta', 0) > 0) |
                        (datos_dia.get('Planificado', 0) > 0)
                    ].copy()
                    
                    
                    if not datos_dia_con_actividad.empty:
                        # Usar la función calcular_disponibilidad_operacional para consistencia con el detalle
                        # Pasar banda_transportista como parámetro
                        resultado_disp = calcular_disponibilidad_operacional(datos_dia_con_actividad, banda_transportista)
                        
                        disp_op_data = {
                            'porcentaje': resultado_disp['disponibilidad_porcentaje'],
                            'turnos_enviados': resultado_disp['turnos_enviados_total'],
                            'entregas_exitosas': resultado_disp['entregas_exitosas_total'],
                            'entregas_criterio_a': resultado_disp['entregas_criterio_a'],
                            'entregas_criterio_b': resultado_disp['entregas_criterio_b']
                        }
                        
                        # Obtener datos de disponibilidad licitada desde la misma función
                        entregas_licitadas = resultado_disp['entregas_licitadas']
                        entregas_totales = resultado_disp['entregas_totales']

            matriz_data['datos'][transportista_nombre][fecha_str] = {
                'porcentaje': round(disponibilidad_banda, 1),
                'total': viajes_realizados,
                'banda': banda_transportista,
                'cumplidos': viajes_realizados,
                'transportista_id': 0,
                'fecha_full': row['Fecha'].strftime('%Y-%m-%d') if hasattr(row['Fecha'], 'strftime') else fecha_actual.strftime('%Y-%m-%d'),
                'disponibilidad_operacional': disp_op_data,
                'entregas_licitadas': entregas_licitadas,
                'entregas_totales': entregas_totales
            }
    
    # Procesar "OTRO TRANSPORTISTA" (medido por entregas, no por banda)
    if not otros_transportistas_data.empty:
        matriz_data['bandas_transportistas']['OTRO TRANSPORTISTA'] = None  # Sin banda
        matriz_data['datos']['OTRO TRANSPORTISTA'] = {}
        
        for _, row in resumen_diario.iterrows():
            fecha_str = row['Fecha'].strftime('%d-%m') if hasattr(row['Fecha'], 'strftime') else row['Fecha']
            fecha_actual = row['Fecha'].date() if hasattr(row['Fecha'], 'date') else row['Fecha']
            datos_otros = otros_transportistas_data[otros_transportistas_data['Fecha'] == row['Fecha']]
            entregas_realizadas = int(datos_otros['Entregado totalmente'].sum())
            
            # Para "otros" no usamos porcentaje de banda, solo mostramos entregas
            matriz_data['datos']['OTRO TRANSPORTISTA'][fecha_str] = {
                'porcentaje': 0,  # No aplica concepto de banda
                'total': entregas_realizadas,
                'banda': None,  # Sin banda asignada
                'cumplidos': entregas_realizadas,
                'transportista_id': 0,
                'fecha_full': row['Fecha'].strftime('%Y-%m-%d') if hasattr(row['Fecha'], 'strftime') else str(row['Fecha']),
                'es_otro': True,  # Marca especial para el frontend
                'disponibilidad_operacional': {'porcentaje': 0, 'turnos_enviados': 0, 'entregas_exitosas': 0},
                'entregas_licitadas': 0,
                'entregas_totales': 0
            }

    return matriz_data


@cache.cached(timeout=1800, key_prefix='transportistas_global')  # Cache por 30 minutos
def obtener_transportistas_global():
    """Obtener transportistas únicos de la tabla completa (para administración)."""
    if not USE_ATHENA:
        return []
    # OPTIMIZACIÓN: Agregar límite y filtro de SIN_INFORMACION en query
    query = f"""
    SELECT DISTINCT carriername1 as transportista 
    FROM {DATABASE_ATHENA}.{TABLA_PEDIDOS} 
    WHERE carriername1 IS NOT NULL 
      AND carriername1 != 'SIN_INFORMACION'
    ORDER BY transportista
    """
    try:
        df = sql_athena(query)
        if df.empty:
            return []
        # Filtrar 'SIN_INFORMACION' de la lista
        transportistas = sorted(df['transportista'].dropna().unique().tolist())
        return [t for t in transportistas if t != 'SIN_INFORMACION']
    except Exception as e:
        print(f"Error obteniendo transportistas desde Athena: {e}")
        return []


# Rutas principales
@app.route('/')
@auth.login_required
def index():
    """Vista principal - Dashboard (lista de mineras predefinidas)"""
    mineras = obtener_mineras_athena()
    data_source = 'Lista predefinida de mineras'
    return render_template('index.html', mineras=mineras, data_source=data_source)


@app.route('/exportar')
@auth.login_required
def exportar():
    """Vista para exportar datos a Excel"""
    mineras = obtener_mineras_athena()
    return render_template('exportar.html', mineras=mineras)


@app.route('/detalle')
@auth.login_required
def detalle():
    """Vista de detalle de transportista (Athena)

    Query params expected:
    - minera: nombre de la minera (string)
    - transportista: nombre del transportista (string)
    - fecha: YYYY-MM-DD
    """
    minera_nombre = request.args.get('minera')
    transportista_nombre = request.args.get('transportista')
    fecha = request.args.get('fecha')

    registros = []
    banda_info = {}
    datos_completos = None
    datos_completos_camiones = []  # Nueva variable para tabla de detalle por camión
    datos_completos_originales = None  # Guardar datos completos sin filtrar
    
    if USE_ATHENA and minera_nombre and fecha and transportista_nombre:
        fecha_obj = datetime.strptime(fecha, '%Y-%m-%d').date()
        
        # Obtener datos completos (SCR + Turnos + RCO)
        datos_completos_originales = obtener_datos_completos_athena(minera_nombre, fecha_obj, fecha_obj)
        datos_completos = datos_completos_originales  # Copiar referencia
        
        if datos_completos is not None and not datos_completos.empty:
            # Manejar el caso especial de "OTRO TRANSPORTISTA"
            if transportista_nombre == 'OTRO TRANSPORTISTA':
                # Filtrar solo transportistas no autorizados
                transportistas_no_autorizados = []
                for transportista in datos_completos['Transportista'].unique():
                    if not es_transportista_autorizado(transportista, minera_nombre):
                        transportistas_no_autorizados.append(transportista)
                                
                datos_completos_filtrados = datos_completos[
                    datos_completos['Transportista'].isin(transportistas_no_autorizados)
                ]
                
                # Para los registros SCR, también incluir todos los transportistas no autorizados
                df_scr = obtener_datos_desde_athena(minera_nombre, fecha_obj, fecha_obj)
                if df_scr is not None and not df_scr.empty:
                    df_scr_filtrado = df_scr[
                        df_scr['Transportista'].isin(transportistas_no_autorizados)
                    ]
                    registros = df_scr_filtrado.to_dict('records')
            else:
                # Caso normal: filtrar por transportista específico   
                datos_completos_filtrados = datos_completos[datos_completos['Transportista'] == transportista_nombre]
                
                # Para mantener compatibilidad con el template existente, 
                # también obtenemos los datos SCR básicos para los registros individuales
                df_scr = obtener_datos_desde_athena(minera_nombre, fecha_obj, fecha_obj)
                if df_scr is not None and not df_scr.empty:
                    df_scr = df_scr[df_scr['Transportista'] == transportista_nombre]
                    registros = df_scr.to_dict('records')
            
            # Calcular bandas por transportista usando todos los transportistas con datos completos
            # NOTA: Esta sección será sobrescrita por la nueva lógica más abajo si hay flota licitada
            if not datos_completos_filtrados.empty and transportista_nombre == 'OTRO TRANSPORTISTA':
                # Solo para OTRO TRANSPORTISTA (fallback)
                transportistas_unicos = datos_completos_filtrados['Transportista'].unique()
                bandas_por_transportista = {}
                
                for transp in transportistas_unicos:
                    bandas_por_transportista[transp] = None  # Sin banda
                
                # Calcular disponibilidad operacional según nueva métrica
                disponibilidad_operacional = calcular_disponibilidad_operacional(datos_completos_filtrados, None)
                
                banda_info = {
                    'banda_total': None,
                    'bandas_por_transportista': bandas_por_transportista,
                    'transportistas_unicos': list(transportistas_unicos),
                    'es_otro_transportista': True,
                    'disponibilidad_operacional': disponibilidad_operacional
                }
                
                # Usar los datos filtrados para el template
                datos_completos = datos_completos_filtrados
        
        # NUEVA LÓGICA: Usar las mismas queries que obtener_actividad_flota_licitada
        # pero filtrada solo por el transportista seleccionado
        if transportista_nombre != 'OTRO TRANSPORTISTA':
            # Obtener flota licitada completa para la minera
            df_flota = obtener_flota_licitada_athena(minera_nombre)
            
            if not df_flota.empty:
                # Extraer palabras clave del nombre del transportista
                palabras_transportista = [
                    palabra for palabra in transportista_nombre.upper().replace('.', '').replace(',', '').split()
                    if len(palabra) > 3
                ]
                
                print(f"\n{'='*80}")
                print(f"🚛 BUSCANDO FLOTA DEL TRANSPORTISTA")
                print(f"   Transportista buscado: {transportista_nombre}")
                print(f"   Palabras clave: {palabras_transportista}")
                
                # Filtrar vehículos del transportista
                def match_transportista(nombre_flota):
                    if pd.isna(nombre_flota):
                        return False
                    nombre_normalizado = nombre_flota.upper().replace('.', '').replace(',', '')
                    return all(palabra in nombre_normalizado for palabra in palabras_transportista)
                
                df_flota_transportista = df_flota[
                    df_flota['nombre_transportista'].apply(match_transportista)
                ].copy()
                
                print(f"   ✓ Total vehículos encontrados en flota: {len(df_flota_transportista)}")
                
                if not df_flota_transportista.empty:
                    vehiculos_transportista = df_flota_transportista['codigo_tanque'].tolist()
                    
                    # 1. Obtener turnos enviados (MISMA QUERY que obtener_actividad_flota_licitada)
                    query_turnos = f"""
                    SELECT id_vehiculo, COUNT(DISTINCT id_turno_uuid) as turnos_enviados
                    FROM (
                        SELECT id_turno_uuid, id_vehiculo, estado,
                               ROW_NUMBER() OVER (PARTITION BY id_turno_uuid ORDER BY fecha_hora DESC) as rn
                        FROM {DATABASE_DISPOMATE}.{TABLA_TURNOS}
                        WHERE DATE(fecha_inicio_utc) >= DATE('{fecha}')
                          AND DATE(fecha_inicio_utc) <= DATE('{fecha}')
                          AND id_vehiculo IN ({','.join(map(str, vehiculos_transportista))})
                    )
                    WHERE rn = 1 AND estado = 'ENVIADO'
                    GROUP BY id_vehiculo
                    """
                    
                    try:
                        df_turnos = sql_athena(query_turnos)
                    except Exception as e:
                        print(f"   ⚠️ Error obteniendo turnos: {e}")
                        df_turnos = pd.DataFrame()
                    
                    # 2. Obtener conexiones RCO (MISMA QUERY que obtener_actividad_flota_licitada)
                    query_rco = f"""
                    SELECT codigo_tanque, COUNT(*) as conexiones_rco
                    FROM {DATABASE_DISPOMATE}.{TABLA_RCO}
                    WHERE DATE(fecha_conexion_utc) >= DATE('{fecha}')
                      AND DATE(fecha_conexion_utc) <= DATE('{fecha}')
                      AND codigo_tanque IN ({','.join(map(str, vehiculos_transportista))})
                    GROUP BY codigo_tanque
                    """
                    
                    try:
                        df_rco = sql_athena(query_rco)
                    except Exception as e:
                        print(f"   ⚠️ Error obteniendo conexiones RCO: {e}")
                        df_rco = pd.DataFrame()
                    
                    # 3. Obtener entregas (MISMA QUERY que obtener_actividad_flota_licitada)
                    query_entregas = f"""
                    SELECT 
                        CAST(vehiclereal AS INTEGER) as vehiclereal,
                        descrstatu as estado,
                        COUNT(*) as cantidad
                    FROM {DATABASE_ATHENA}.{TABLA_PEDIDOS}
                    WHERE vdatu >= '{fecha}'
                      AND vdatu <= '{fecha}'
                      AND CAST(vehiclereal AS INTEGER) IN ({','.join(map(str, vehiculos_transportista))})
                    GROUP BY CAST(vehiclereal AS INTEGER), descrstatu
                    """
                    
                    try:
                        df_entregas = sql_athena(query_entregas)
                        if not df_entregas.empty:
                            df_entregas['vehiclereal'] = df_entregas['vehiclereal'].astype(int)
                    except Exception as e:
                        print(f"   ⚠️ Error obteniendo entregas: {e}")
                        df_entregas = pd.DataFrame()
                    
                    # 4. Combinar información para cada vehículo
                    for _, vehiculo in df_flota_transportista.iterrows():
                        codigo_tanque = int(vehiculo['codigo_tanque'])
                        
                        # Turnos enviados
                        turnos_enviados = 0
                        if not df_turnos.empty and 'id_vehiculo' in df_turnos.columns:
                            turno_row = df_turnos[df_turnos['id_vehiculo'] == codigo_tanque]
                            if not turno_row.empty:
                                turnos_enviados = int(turno_row['turnos_enviados'].iloc[0])
                        
                        # Conexiones RCO
                        conexion_rco = 0
                        if not df_rco.empty and 'codigo_tanque' in df_rco.columns:
                            rco_row = df_rco[df_rco['codigo_tanque'] == codigo_tanque]
                            if not rco_row.empty:
                                conexion_rco = int(rco_row['conexiones_rco'].iloc[0])
                        
                        # Entregas
                        entregado_totalmente = 0
                        en_ruta = 0
                        planificado = 0
                        
                        if not df_entregas.empty:
                            entregas_vehiculo = df_entregas[df_entregas['vehiclereal'] == codigo_tanque]
                            
                            if not entregas_vehiculo.empty:
                                # Entregado totalmente
                                entregas_completadas = entregas_vehiculo[
                                    entregas_vehiculo['estado'].isin(['Entregado totalmente', 'Recibí Conforme'])
                                ]
                                entregado_totalmente = int(entregas_completadas['cantidad'].sum()) if not entregas_completadas.empty else 0
                                
                                # En Ruta
                                en_ruta_rows = entregas_vehiculo[entregas_vehiculo['estado'] == 'En Ruta']
                                en_ruta = int(en_ruta_rows['cantidad'].sum()) if not en_ruta_rows.empty else 0
                                
                                # Planificado
                                planificado_rows = entregas_vehiculo[entregas_vehiculo['estado'] == 'Planificado']
                                planificado = int(planificado_rows['cantidad'].sum()) if not planificado_rows.empty else 0
                        
                        # LÓGICA DE DISPONIBILIDAD (2 criterios activos):
                        disponible = 0
                        if entregado_totalmente > 0:
                            disponible = 1
                        elif conexion_rco > 0:
                            disponible = 1
                        
                        datos_completos_camiones.append({
                            'Camion': codigo_tanque,
                            'Fecha': fecha,
                            'Es_licitado': 'Si',
                            'Transportista': vehiculo['nombre_transportista'],
                            'Turnos_enviados': turnos_enviados,
                            'Conexion_RCO': conexion_rco,
                            'Entregado_totalmente': entregado_totalmente,
                            'En_ruta': en_ruta,
                            'Planificado': planificado,
                            'Disponible': disponible
                        })
                    
                    print(f"   ✅ Procesados {len(datos_completos_camiones)} vehículos")
                    print(f"{'='*80}\n")
                    
                    # Calcular KPI de Disponibilidad Operacional con los datos correctos
                    if datos_completos_camiones:
                        # Sumar total de camiones disponibles
                        camiones_disponibles = sum(camion['Disponible'] for camion in datos_completos_camiones)
                        
                        # Obtener banda del transportista
                        banda_transportista = calcular_banda_transportista(transportista_nombre, minera_nombre)
                        
                        # Calcular porcentaje
                        if banda_transportista and banda_transportista > 0:
                            disponibilidad_porcentaje = (camiones_disponibles / banda_transportista) * 100
                            disponibilidad_porcentaje = min(100.0, disponibilidad_porcentaje)
                        else:
                            disponibilidad_porcentaje = 0.0
                        
                        # Calcular disponibilidad licitada (todos los camiones en datos_completos_camiones son licitados)
                        entregas_totales = sum(camion['Entregado_totalmente'] for camion in datos_completos_camiones)
                        entregas_licitadas = entregas_totales  # Todos son licitados
                        
                        disponibilidad_licitada_porcentaje = 100.0 if entregas_totales > 0 else 0.0
                        
                        # Crear estructura de disponibilidad operacional
                        disponibilidad_operacional = {
                            'disponibilidad_porcentaje': round(disponibilidad_porcentaje, 1),
                            'camiones_disponibles': camiones_disponibles,
                            'banda_total': banda_transportista,
                            'entregas_exitosas_total': camiones_disponibles,  # Para compatibilidad
                            'entregas_criterio_a': sum(1 for c in datos_completos_camiones if c['Entregado_totalmente'] > 0),
                            'entregas_criterio_b': sum(1 for c in datos_completos_camiones if c['Entregado_totalmente'] == 0 and c['Conexion_RCO'] > 0),
                            'turnos_enviados_total': sum(camion['Turnos_enviados'] for camion in datos_completos_camiones),
                            'entregas_licitadas': entregas_licitadas,
                            'entregas_totales': entregas_totales,
                            'disponibilidad_licitada_porcentaje': round(disponibilidad_licitada_porcentaje, 1)
                        }
                        
                        banda_info = {
                            'banda_total': banda_transportista,
                            'bandas_por_transportista': {transportista_nombre: banda_transportista},
                            'transportistas_unicos': [transportista_nombre],
                            'es_otro_transportista': False,
                            'disponibilidad_operacional': disponibilidad_operacional
                        }
                        

    # Obtener información de flota licitada con actividad detallada para la minera
    flota_licitada = []
    if USE_ATHENA and minera_nombre and fecha:
        fecha_obj = datetime.strptime(fecha, '%Y-%m-%d').date()
        flota_licitada = obtener_actividad_flota_licitada(minera_nombre, fecha_obj, fecha_obj)
    elif USE_ATHENA and minera_nombre:
        # Si no hay fecha, solo mostrar la flota sin actividad
        df_flota = obtener_flota_licitada_athena(minera_nombre)
        if not df_flota.empty:
            flota_licitada = df_flota.to_dict('records')
    
    mineras = obtener_mineras_athena()
    transportistas = obtener_transportistas_global() if USE_ATHENA else []

    return render_template('detalle.html', 
                         minera=minera_nombre, 
                         transportista=transportista_nombre,
                         fecha=fecha,
                         registros=registros,
                         datos_completos=datos_completos_camiones if datos_completos_camiones else [],
                         banda_info=banda_info,
                         flota_licitada=flota_licitada,
                         mineras=mineras,
                         transportistas=transportistas)


# ============================================
# FUNCIÓN DE EXPORTACIÓN MATRICIAL XLSX
# ============================================
def exportar_xlsx_matricial(df_export, fecha_inicio, fecha_fin, output_buffer, minera_nombre):
    """
    Generar archivo XLSX con formato matricial por transportista LICITADO
    Solo incluye transportistas que tienen vehículos licitados
    
    Args:
        df_export: DataFrame con los datos
        fecha_inicio: datetime - Fecha de inicio
        fecha_fin: datetime - Fecha de fin
        output_buffer: BytesIO buffer donde guardar el archivo
        minera_nombre: str - Nombre de la minera para obtener bandas
    """
    
    # Crear workbook
    workbook = openpyxl.Workbook()
    workbook.remove(workbook.active)  # Remover hoja por defecto
    
    # Filtrar solo transportistas LICITADOS
    df_licitados = df_export[df_export['Es_Licitado'] == 'Si'].copy()
    
    if df_licitados.empty:
        # Crear hoja con mensaje
        ws = workbook.create_sheet(title="Sin Datos")
        ws['A1'] = "No se encontraron transportistas con flota licitada para esta minera"
        workbook.save(output_buffer)
        return
    
    # Obtener solo transportistas que tienen vehículos licitados
    transportistas_licitados = sorted(df_licitados['Transportista'].unique())
    
    # Separar datos de transportistas NO licitados para hoja separada
    df_no_licitados = df_export[df_export['Es_Licitado'] == 'No'].copy()
    
    # Obtener rango de fechas
    fechas = pd.date_range(start=fecha_inicio, end=fecha_fin, freq='D')
    num_dias = len(fechas)
    
    # Estilos
    header_fill = PatternFill(start_color="0066CC", end_color="0066CC", fill_type="solid")
    header_font = Font(bold=True, color="FFFFFF")
    center_alignment = Alignment(horizontal="center", vertical="center")
    thin_border = Border(
        left=Side(style='thin'),
        right=Side(style='thin'),
        top=Side(style='thin'),
        bottom=Side(style='thin')
    )
    
    # Crear una hoja por transportista LICITADO
    for transportista in transportistas_licitados:
        # Filtrar SOLO datos de flota licitada del transportista
        df_transp = df_licitados[df_licitados['Transportista'] == transportista].copy()
        
        # Crear worksheet
        ws = workbook.create_sheet(title=transportista[:31])  # Límite de 31 caracteres
        
        # SECCIÓN DE CONFIGURACIÓN
        ws.merge_cells('B1:H1')
        ws.merge_cells('B2:H2')
        cell_config = ws['B1']
        cell_config.value = 'Configuración de banda minima'
        cell_config.font = Font(bold=True)
        
        ws.merge_cells('I1:P1')
        ws.merge_cells('I2:P2')
        cell_max = ws['I1']
        cell_max.value = 'Configuración de banda maxima'
        cell_max.font = Font(bold=True)
              
        # Fila 2 - Valores de configuración (banda del transportista)
        banda_transportista = calcular_banda_transportista(transportista, minera_nombre)
        if banda_transportista:
            ws['B2'] = banda_transportista
            ws['I2'] = banda_transportista
        else:
            ws['B2'] = 0
            ws['I2'] = 0
        
        # Columna de Disponibilidad del periodo
        col_disp_start = 22 + (num_dias * 3)
        ws.cell(row=1, column=col_disp_start).value = 'Disponibilidad del periodo'
        ws.cell(row=1, column=col_disp_start).font = Font(bold=True)
        
        # Calcular disponibilidad del transportista
        resultado_disp = calcular_disponibilidad_operacional(df_transp)
        ws.cell(row=2, column=col_disp_start).value = resultado_disp['disponibilidad_porcentaje'] / 100
        ws.cell(row=2, column=col_disp_start).number_format = '0.00'
        
        # ENCABEZADOS DE DÍAS
        row_dia = 5
        row_criterio = 6
        
        ws['A5'] = 'Día'
        ws['A6'] = 'Camion \\ Criterio'
        
        # Configurar encabezados de días y criterios
        current_col = 2  # Columna B
        for fecha in fechas:
            dia_numero = fecha.day
            
            # Día (se repite en 3 columnas)
            ws.merge_cells(start_row=row_dia, start_column=current_col, 
                          end_row=row_dia, end_column=current_col + 2)
            cell_dia = ws.cell(row=row_dia, column=current_col)
            cell_dia.value = dia_numero
            cell_dia.alignment = center_alignment
            
            # Criterios 1, 2, 3
            for criterio in [1, 2, 3]:
                ws.cell(row=row_criterio, column=current_col).value = criterio
                ws.cell(row=row_criterio, column=current_col).alignment = center_alignment
                current_col += 1
        
        # Aplicar formato a encabezados
        for col in range(1, current_col):
            for row in [row_dia, row_criterio]:
                cell = ws.cell(row=row, column=col)
                cell.font = header_font
                cell.fill = header_fill
                cell.alignment = center_alignment
        
        # DATOS DE CAMIONES
        camiones = sorted(df_transp['Camion'].unique())
        
        for idx_camion, camion in enumerate(camiones):
            row_actual = 7 + idx_camion
            
            # Nombre del camión en columna A
            ws.cell(row=row_actual, column=1).value = int(camion)
            
            # Filtrar datos del camión
            df_camion = df_transp[df_transp['Camion'] == camion].copy()
            
            # Crear diccionario fecha -> datos para acceso rápido
            df_camion['fecha_key'] = pd.to_datetime(df_camion['fecha_completa']).dt.date
            datos_por_fecha = df_camion.set_index('fecha_key').to_dict('index')
            
            # Llenar datos por fecha
            current_col = 2
            for fecha in fechas:
                fecha_key = fecha.date()
                
                # Obtener datos del día (si existen)
                if fecha_key in datos_por_fecha:
                    datos_dia = datos_por_fecha[fecha_key]
                    entregado = datos_dia.get('Entregado_totalmente', 0)
                    conexion_rco = datos_dia.get('Conexion_RCO', 0)
                    en_ruta = datos_dia.get('En_ruta', 0)
                    planificado = datos_dia.get('Planificado', 0)
                    
                    # Criterio 1: Entrega realizada (Entregado_totalmente > 0)
                    criterio_1 = 1 if entregado > 0 else 0
                    
                    # Criterio 2: No entregado PERO con conexión RCO
                    criterio_2 = 1 if (entregado == 0 and conexion_rco > 0) else 0
                    
                    # Criterio 3: No entregado, sin RCO PERO con estadía en planta
                    criterio_3 = 1 if (entregado == 0 and conexion_rco == 0 and (en_ruta > 0 or planificado > 0)) else 0
                else:
                    # Sin datos para este día
                    criterio_1 = 0
                    criterio_2 = 0
                    criterio_3 = 0
                
                # Escribir valores
                ws.cell(row=row_actual, column=current_col).value = criterio_1
                ws.cell(row=row_actual, column=current_col + 1).value = criterio_2
                ws.cell(row=row_actual, column=current_col + 2).value = criterio_3
                
                current_col += 3
        
        # FILA DE TOTALES POR CRITERIO
        row_totales = 7 + len(camiones)
        
        # Etiqueta de totales
        cell_totales = ws.cell(row=row_totales, column=1)
        cell_totales.value = "TOTAL"
        cell_totales.font = Font(bold=True)
        cell_totales.alignment = center_alignment
        
        # Calcular totales por día y criterio
        current_col = 2
        for fecha in fechas:
            # Para cada día, sumar todos los camiones
            for offset_criterio in range(3):  # 3 criterios
                # Construir fórmula de suma para esta columna
                primera_fila_datos = 7
                ultima_fila_datos = 7 + len(camiones) - 1
                col_letter = openpyxl.utils.get_column_letter(current_col)
                
                formula = f"=SUM({col_letter}{primera_fila_datos}:{col_letter}{ultima_fila_datos})"
                cell_total = ws.cell(row=row_totales, column=current_col)
                cell_total.value = formula
                cell_total.font = Font(bold=True)
                cell_total.alignment = center_alignment
                
                current_col += 1
        
        # FILA DE TOTAL GENERAL (SUMA DE TODOS LOS CRITERIOS)
        row_total_general = row_totales + 1
        
        # Etiqueta
        cell_total_general = ws.cell(row=row_total_general, column=1)
        cell_total_general.value = "TOTAL CRITERIOS"
        cell_total_general.font = Font(bold=True)
        cell_total_general.alignment = center_alignment
        cell_total_general.fill = PatternFill(start_color="CCCCCC", end_color="CCCCCC", fill_type="solid")
        
        # Calcular total general por día (suma de los 3 criterios)
        current_col = 2
        for fecha in fechas:
            # Sumar los 3 criterios del día
            col1_letter = openpyxl.utils.get_column_letter(current_col)
            col2_letter = openpyxl.utils.get_column_letter(current_col + 1)
            col3_letter = openpyxl.utils.get_column_letter(current_col + 2)
            
            formula = f"={col1_letter}{row_totales}+{col2_letter}{row_totales}+{col3_letter}{row_totales}"
            
            # Colocar el total en la primera columna del día y hacer merge de las 3 columnas
            ws.merge_cells(start_row=row_total_general, start_column=current_col,
                          end_row=row_total_general, end_column=current_col + 2)
            
            cell_general = ws.cell(row=row_total_general, column=current_col)
            cell_general.value = formula
            cell_general.font = Font(bold=True, size=11)
            cell_general.alignment = center_alignment
            cell_general.fill = PatternFill(start_color="CCCCCC", end_color="CCCCCC", fill_type="solid")
            
            current_col += 3
        
        # APLICAR BORDES Y AJUSTAR COLUMNAS
        # Ajustar ancho de columnas
        ws.column_dimensions['A'].width = 20
        for col in range(2, current_col):
            col_letter = openpyxl.utils.get_column_letter(col)
            ws.column_dimensions[col_letter].width = 4
        
        # Aplicar bordes a la tabla de datos (incluye filas de totales)
        max_data_row = 7 + len(camiones) + 1  # +2 para incluir ambas filas de totales
        for row in range(row_dia, max_data_row + 1):
            for col in range(1, current_col):
                ws.cell(row=row, column=col).border = thin_border
    
    # HOJAS ADICIONALES: LOGS (SOLO FLOTA LICITADA)
    
    # HOJA: LOG - Entregas Licitadas
    ws_entregas = workbook.create_sheet(title='LOG - Entregas Licitadas')
    entregas_data = df_licitados[df_licitados['Entregado_totalmente'] > 0][
        ['Fecha', 'Camion', 'Transportista', 'Entregado_totalmente']
    ].copy()
    
    for r_idx, row in enumerate(dataframe_to_rows(entregas_data, index=False, header=True), 1):
        for c_idx, value in enumerate(row, 1):
            cell = ws_entregas.cell(row=r_idx, column=c_idx, value=value)
            if r_idx == 1:  # Encabezado
                cell.font = header_font
                cell.fill = header_fill
                cell.alignment = center_alignment
    
    # HOJA: LOG - RCO Licitadas
    ws_rco = workbook.create_sheet(title='LOG - RCO Licitadas')
    rco_data = df_licitados[df_licitados['Conexion_RCO'] > 0][
        ['Fecha', 'Camion', 'Transportista', 'Conexion_RCO']
    ].copy()
    
    for r_idx, row in enumerate(dataframe_to_rows(rco_data, index=False, header=True), 1):
        for c_idx, value in enumerate(row, 1):
            cell = ws_rco.cell(row=r_idx, column=c_idx, value=value)
            if r_idx == 1:  # Encabezado
                cell.font = header_font
                cell.fill = header_fill
                cell.alignment = center_alignment
    
    # HOJA: LOG - Eventos Licitadas
    ws_eventos = workbook.create_sheet(title='LOG - Eventos Licitadas')
    eventos_data = df_licitados[
        ['Fecha', 'Camion', 'Transportista', 
         'Turnos_enviados', 'Conexion_RCO', 'Entregado_totalmente', 'En_ruta', 'Planificado']
    ].copy()
    
    for r_idx, row in enumerate(dataframe_to_rows(eventos_data, index=False, header=True), 1):
        for c_idx, value in enumerate(row, 1):
            cell = ws_eventos.cell(row=r_idx, column=c_idx, value=value)
            if r_idx == 1:  # Encabezado
                cell.font = header_font
                cell.fill = header_fill
                cell.alignment = center_alignment
    
    # HOJA ADICIONAL: OTROS TRANSPORTISTAS (NO LICITADOS)
    if not df_no_licitados.empty:
        ws_otros = workbook.create_sheet(title='OTROS Transportistas')
        
        # Agrupar datos de no licitados por transportista y fecha
        resumen_otros = df_no_licitados.groupby(['Transportista', 'Fecha']).agg({
            'Camion': 'count',  # Cantidad de camiones
            'Turnos_enviados': 'sum',
            'Conexion_RCO': 'sum',
            'Entregado_totalmente': 'sum'
        }).reset_index()
        resumen_otros.columns = ['Transportista', 'Fecha', 'Cantidad_Camiones', 
                                  'Turnos_enviados', 'Conexion_RCO', 'Entregado_totalmente']
        
        for r_idx, row in enumerate(dataframe_to_rows(resumen_otros, index=False, header=True), 1):
            for c_idx, value in enumerate(row, 1):
                cell = ws_otros.cell(row=r_idx, column=c_idx, value=value)
                if r_idx == 1:  # Encabezado
                    cell.font = header_font
                    cell.fill = header_fill
                    cell.alignment = center_alignment
    
    # Guardar workbook
    workbook.save(output_buffer)


@app.route('/api/dashboard_data')
@auth.login_required
def dashboard_data():
    """API endpoint para obtener datos del dashboard (Athena-only)

    Query params expected:
    - minera: nombre de la minera (string)
    - mes: mes (int)
    - semana: semana dentro del mes (int)
    - año: año (int, opcional, por defecto año actual)
    Optional:
    - viajes_min: override mínimo de viajes
    - viajes_max: override máximo de viajes
    """
    minera_nombre = request.args.get('minera')
    mes = request.args.get('mes', type=int)
    semana = request.args.get('semana', type=int)
    año = request.args.get('año', type=int, default=datetime.now().year)
    
    # Obtener configuración específica para la minera
    config_viajes = obtener_configuracion_viajes()
    minera_config = config_viajes.get(minera_nombre, {'minimo': 11, 'maximo': 13})
    
    # Permitir override desde parámetros de la URL
    viajes_min = request.args.get('viajes_min', type=int, default=minera_config['minimo'])
    viajes_max = request.args.get('viajes_max', type=int, default=minera_config['maximo'])

    if not USE_ATHENA:
        return jsonify({'error': 'Athena no está disponible en este entorno'}), 503

    if not minera_nombre or not mes or not semana:
        return jsonify({'error': 'Parámetros incompletos'}), 400

    fecha_inicio, fecha_fin = calcular_rango_semana(año, mes, semana)

    grafico_data = obtener_datos_grafico_athena(minera_nombre, fecha_inicio, fecha_fin, viajes_min, viajes_max)
    matriz_data = obtener_datos_matriz_athena(minera_nombre, fecha_inicio, fecha_fin, viajes_min, viajes_max)

    # Agregar información de bandas por transportista al grafico_data
    bandas_por_transportista = {}
    for transportista in grafico_data.get('transportistas', []):
        if transportista != 'OTRO TRANSPORTISTA':
            banda = calcular_banda_transportista(transportista, minera_nombre)
            bandas_por_transportista[transportista] = banda
        else:
            bandas_por_transportista[transportista] = None
    
    grafico_data['bandas_transportista'] = bandas_por_transportista

    return jsonify({
        'grafico': grafico_data,
        'matriz': matriz_data,
        'minera': {
            'nombre': minera_nombre,
            'viajes_minimos': viajes_min,
            'viajes_maximos': viajes_max
        },
        'source': 'Athena'
    })


@app.route('/api/semanas/<int:mes>')
@auth.login_required
def obtener_semanas(mes):
    """Obtener semanas disponibles para un mes"""
    año = request.args.get('año', type=int, default=datetime.now().year)
    semanas = calcular_semanas_mes(año, mes)
    return jsonify({'semanas': semanas})


def calcular_semanas_mes(año, mes):
    """Calcular número de semanas en un mes"""
    from calendar import monthrange
    dias_mes = monthrange(año, mes)[1]
    semanas = (dias_mes + 6) // 7
    return list(range(1, semanas + 1))


def calcular_rango_semana(año, mes, semana):
    """Calcular fecha de inicio y fin de una semana dentro de un mes"""
    primer_dia = datetime(año, mes, 1).date()
    dias_desde_inicio = (semana - 1) * 7
    fecha_inicio = primer_dia + timedelta(days=dias_desde_inicio)
    fecha_fin = fecha_inicio + timedelta(days=6)
    
    from calendar import monthrange
    ultimo_dia_mes = monthrange(año, mes)[1]
    fecha_fin_mes = datetime(año, mes, ultimo_dia_mes).date()
    
    if fecha_fin > fecha_fin_mes:
        fecha_fin = fecha_fin_mes
    
    return fecha_inicio, fecha_fin


# Removed SQLite-based helpers. Use Athena helpers: obtener_datos_grafico_athena and obtener_datos_matriz_athena

@app.route('/admin/mineras')
@auth.login_required
def admin_mineras():
    """Gestión de mineras y asociaciones"""
    mineras = obtener_mineras_athena()
    transportistas = obtener_transportistas_global() if USE_ATHENA else []
    return render_template('admin_mineras.html', mineras=mineras, transportistas=transportistas)


@app.route('/api/debug/transportistas')
@auth.login_required
def debug_transportistas():
    """Ruta temporal para identificar nombres exactos de transportistas"""
    if not USE_ATHENA:
        return jsonify({'error': 'Athena no está disponible'}), 503
    
    query = f"""
    SELECT DISTINCT carriername1 as transportista, COUNT(*) as total_registros
    FROM {DATABASE_ATHENA}.{TABLA_PEDIDOS}
    WHERE carriername1 IS NOT NULL 
    AND carriername1 != 'SIN_INFORMACION'
    GROUP BY carriername1
    ORDER BY total_registros DESC, transportista
    LIMIT 50
    """
    
    try:
        df = sql_athena(query)
        if df.empty:
            return jsonify({'transportistas': [], 'mensaje': 'No se encontraron transportistas'})
        
        # Convertir a lista de diccionarios
        transportistas_info = []
        for _, row in df.iterrows():
            transportistas_info.append({
                'nombre': row['transportista'],
                'registros': int(row['total_registros'])
            })
        
        return jsonify({
            'transportistas': transportistas_info,
            'total_encontrados': len(transportistas_info),
            'mensaje': 'Transportistas ordenados por cantidad de registros'
        })
        
    except Exception as e:
        return jsonify({'error': f'Error obteniendo transportistas: {str(e)}'}), 500


@app.route('/api/exportar_xlsx')
@auth.login_required
def exportar_xlsx():
    """Generar archivo XLSX con formato matricial para rango de fechas"""
    try:
        minera = request.args.get('minera')
        fecha_inicio_str = request.args.get('fecha_inicio')
        fecha_fin_str = request.args.get('fecha_fin')
        
        if not minera or not fecha_inicio_str or not fecha_fin_str:
            return jsonify({'error': 'Parámetros requeridos: minera, fecha_inicio, fecha_fin'}), 400
        
        # Convertir fechas
        try:
            fecha_inicio = datetime.strptime(fecha_inicio_str, '%Y-%m-%d')
            fecha_fin = datetime.strptime(fecha_fin_str, '%Y-%m-%d')
        except ValueError:
            return jsonify({'error': 'Formato de fecha inválido. Use YYYY-MM-DD'}), 400
        
        if fecha_inicio > fecha_fin:
            return jsonify({'error': 'La fecha de inicio no puede ser mayor a la fecha de fin'}), 400
        
        # Obtener datos completos para el rango de fechas
        datos_completos = obtener_datos_completos_athena(minera, fecha_inicio, fecha_fin)
        
        if datos_completos is None or datos_completos.empty:
            return jsonify({'error': 'No se encontraron datos para los parámetros especificados'}), 404
        
        # Obtener vehículos licitados
        vehiculos_licitados = obtener_vehiculos_licitados_por_minera(minera)
        
        # Preparar DataFrame para exportación
        df_export = datos_completos.copy()
        
        # Agregar columna de vehículo licitado si no existe
        if '¿Es licitado?' in df_export.columns:
            df_export['Es_Licitado'] = df_export['¿Es licitado?']
        else:
            df_export['Es_Licitado'] = df_export['Camion'].isin(vehiculos_licitados).map({True: 'Si', False: 'No'})
        
        # Crear archivo Excel en memoria
        output = BytesIO()
        
        # Generar Excel con formato matricial
        exportar_xlsx_matricial(df_export, fecha_inicio, fecha_fin, output, minera)
        
        output.seek(0)
        
        # Preparar respuesta
        filename = f"DispoMinera_{minera}_{fecha_inicio_str}_{fecha_fin_str}.xlsx"
        
        return send_file(
            output,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name=filename
        )
        
    except Exception as e:
        print(f"Error generando XLSX: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': f'Error generando archivo: {str(e)}'}), 500


@app.route('/api/cache/clear', methods=['POST'])
@auth.login_required
def clear_cache():
    """Endpoint para limpiar el cache manualmente"""
    try:
        cache.clear()
        return jsonify({
            'success': True,
            'mensaje': 'Cache limpiado exitosamente'
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/cache/stats', methods=['GET'])
@auth.login_required
def cache_stats():
    """Obtener estadísticas del cache (solo disponible con algunos backends)"""
    stats = {
        'cache_type': app.config['CACHE_TYPE'],
        'default_timeout': app.config['CACHE_DEFAULT_TIMEOUT'],
        'mensaje': 'Para estadísticas detalladas, use Redis como backend'
    }
    
    # Intentar obtener info adicional si es posible
    try:
        if hasattr(cache.cache, '_cache'):
            stats['cached_keys_count'] = len(cache.cache._cache)
            stats['cached_keys'] = list(cache.cache._cache.keys())[:10]  # Primeras 10
    except:
        pass
    
    return jsonify(stats)


@app.route('/api/mineras', methods=['GET', 'POST'])
@auth.login_required
def api_mineras():
    """Endpoints de mineras — Athena-only read API. POST no soportado."""
    if request.method == 'GET':
        mineras = obtener_mineras_athena()
        config_viajes = obtener_configuracion_viajes()
        
        # Devolver lista con configuración específica de cada minera
        return jsonify([{
            'nombre': m,
            'viajes_minimos': config_viajes.get(m, {'minimo': 11})['minimo'],
            'viajes_maximos': config_viajes.get(m, {'maximo': 13})['maximo']
        } for m in mineras])

    elif request.method == 'POST':
        return jsonify({'error': 'Creación de mineras no soportada en modo Athena-only'}), 405


if __name__ == '__main__':
    # Configuración para producción en Render
    port = int(os.getenv('PORT', 5000))
    debug = os.getenv('FLASK_ENV', 'development') == 'development'
    
    print(f"🚀 Iniciando aplicación en puerto {port}")
    print(f"🔧 Debug mode: {debug}")
    print(f"⚡ Athena disponible: {ATHENA_AVAILABLE}")
    print(f"🔐 Autenticación HTTP Basic activada")
    
    app.run(host='0.0.0.0', port=port, debug=debug)