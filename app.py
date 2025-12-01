from flask import Flask, render_template, request, jsonify
from flask_httpauth import HTTPBasicAuth
from werkzeug.security import generate_password_hash, check_password_hash
from datetime import datetime, timedelta
import os
import pandas as pd
import numpy as np
from dotenv import load_dotenv

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


# Forzar por defecto el uso de Athena si la librería está disponible.
# Si pyathena no está instalado, USE_ATHENA será False y la app informará al usuario.
USE_ATHENA = os.getenv('USE_ATHENA', 'true').lower() == 'true' and ATHENA_AVAILABLE

# Conexión global a Athena
athena_conn = None

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


# NOTE: Removing SQLAlchemy models — the app now works exclusively with Athena.


def obtener_datos_completos_athena(minera_nombre, fecha_inicio, fecha_fin):
    """
    Obtener datos completos integrando SCR, Turnos y RCO (similar al notebook)
    """
    if not USE_ATHENA:
        return None

    # 1. Obtener datos SCR (pedidos)
    df_scr = obtener_datos_desde_athena(minera_nombre, fecha_inicio, fecha_fin)
    if df_scr is None or df_scr.empty:
        return None

    # 2. Obtener turnos enviados
    query_turnos = f"""
    SELECT * 
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
    query_rco = f"""
    SELECT * 
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
    resultado_completo = procesar_datos_completos(df_scr, df_turnos, df_rco, fecha_inicio, fecha_fin)
    
    return resultado_completo


def calcular_disponibilidad_operacional(datos_completos):
    """
    Calcular la Disponibilidad Operacional según nueva métrica:
    
    Disponibilidad = (Entregas Exitosas) / (Total Turnos Enviados)
    
    Entregas Exitosas incluyen:
    - Criterio A: Pedido fue entregado correctamente (Entregado_totalmente > 0)
    - Criterio B: Pedido NO fue entregado PERO sí hubo conexión RCO (Entregado_totalmente == 0 AND Conexion_RCO > 0)
    
    Definiciones técnicas:
    - "Entregado Correctamente": Estado 'Entregado totalmente' o 'Recibí Conforme' en SCR
    - "Conexiones al RCO": Registro de conexión operacional en tabla RCO (cualquier evento > 0)
    
    Args:
        datos_completos: DataFrame con columnas ['Turnos_enviados', 'Entregado_totalmente', 'Conexion_RCO']
    
    Returns:
        dict con:
            - entregas_exitosas_total: Total de entregas consideradas exitosas
            - entregas_criterio_a: Entregas completadas correctamente
            - entregas_criterio_b: Entregas fallidas pero con conexión RCO
            - turnos_enviados_total: Total de turnos enviados
            - disponibilidad_porcentaje: Porcentaje de disponibilidad operacional
    """
    if datos_completos is None or datos_completos.empty:
        return {
            'entregas_exitosas_total': 0,
            'entregas_criterio_a': 0,
            'entregas_criterio_b': 0,
            'turnos_enviados_total': 0,
            'disponibilidad_porcentaje': 0.0
        }
    
    # Criterio A: Entregado correctamente
    entregas_criterio_a = int(datos_completos['Entregado_totalmente'].sum())
    
    # Criterio B: No entregado PERO con conexión RCO
    # Identificar registros donde NO se entregó (Entregado_totalmente == 0) PERO hubo conexión RCO (Conexion_RCO > 0)
    criterio_b_mask = (datos_completos['Entregado_totalmente'] == 0) & (datos_completos['Conexion_RCO'] > 0)
    entregas_criterio_b = int(criterio_b_mask.sum())
    
    # Total de entregas exitosas (Criterio A ∪ Criterio B)
    entregas_exitosas_total = entregas_criterio_a + entregas_criterio_b
    
    # Total de turnos enviados
    turnos_enviados_total = int(datos_completos['Turnos_enviados'].sum())
    
    # Calcular porcentaje de disponibilidad
    if turnos_enviados_total > 0:
        disponibilidad_porcentaje = (entregas_exitosas_total / turnos_enviados_total) * 100
    else:
        disponibilidad_porcentaje = 0.0
    
    return {
        'entregas_exitosas_total': entregas_exitosas_total,
        'entregas_criterio_a': entregas_criterio_a,
        'entregas_criterio_b': entregas_criterio_b,
        'turnos_enviados_total': turnos_enviados_total,
        'disponibilidad_porcentaje': round(disponibilidad_porcentaje, 2)
    }


def procesar_datos_completos(df_scr, df_turnos, df_rco, fecha_inicio, fecha_fin):
    """
    Procesar y combinar datos de SCR, Turnos y RCO (replicando lógica del notebook)
    """
    # Obtener vehículos únicos del SCR
    vehiculos_totales = df_scr['vehiclereal'].dropna().unique()
    vehiculos_totales = np.sort(vehiculos_totales.astype(int))
    
    # Lista de vehículos licitados (basada en el notebook)
    vehiculos_licitados = [4002, 4003, 4049, 8054, 8120, 8348, 8820]
    
    # Crear tabla base combinando todos los vehículos con todas las fechas
    fechas_rango = pd.date_range(start=fecha_inicio, end=fecha_fin, freq='D')
    
    base_data = []
    for vehiculo in vehiculos_totales:
        for fecha in fechas_rango:
            base_data.append({
                'Camion': int(vehiculo),
                'Fecha': fecha.strftime('%d-%b'),
                'fecha_completa': fecha,
                'fecha_para_merge': fecha.date()
            })
    
    tabla_base = pd.DataFrame(base_data)
    tabla_base['¿Es licitado?'] = np.where(tabla_base['Camion'].isin(vehiculos_licitados), 'Si', 'No')
    
    # Procesar turnos enviados
    if not df_turnos.empty:
        # Obtener estado final por turno
        estado_final = (
            df_turnos.sort_values(['id_turno_uuid', 'fecha_hora'])
                     .groupby('id_turno_uuid')
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
        df_rco_filtrado['fecha_rco'] = pd.to_datetime(df_rco_filtrado['fecha_carga_particion']).dt.date
        
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
    
    pivot_scr['vehiclereal'] = pivot_scr['vehiclereal'].astype(int)
    
    # Obtener transportista por vehículo y fecha
    carriers = (
        df_scr[['vehiclereal', 'fecha_corregida', 'carriername1']]
        .dropna(subset=['carriername1'])
        .drop_duplicates(subset=['vehiclereal', 'fecha_corregida'])
        .groupby(['vehiclereal', 'fecha_corregida'], as_index=False).first()
    )
    carriers['vehiclereal'] = carriers['vehiclereal'].astype(int)
    
    # Crear fecha para merge
    tabla_base['fecha_merge_scr'] = pd.to_datetime(tabla_base['Fecha'] + '-2025', format='%d-%b-%Y').dt.strftime('%Y-%m-%d')
    
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
def obtener_datos_desde_athena(minera_nombre, fecha_inicio, fecha_fin):
    """
    Obtener datos desde Athena siguiendo la lógica de athena_test.py
    """
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
        
        query = f"""
        SELECT *
        FROM {DATABASE_ATHENA}.{TABLA_PEDIDOS}
        WHERE vdatu >= '{fecha_inicio.strftime('%Y-%m-%d')}'
          AND vdatu <= '{fecha_fin.strftime('%Y-%m-%d')}'
          AND ({' OR '.join(conditions)})
        """
    else:
        # Caso normal para mineras individuales
        minera_safe = minera_nombre.replace("'", "''")
        query = f"""
        SELECT *
        FROM {DATABASE_ATHENA}.{TABLA_PEDIDOS}
        WHERE vdatu >= '{fecha_inicio.strftime('%Y-%m-%d')}'
          AND vdatu <= '{fecha_fin.strftime('%Y-%m-%d')}'
          AND vtext LIKE '{minera_safe}'
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


def obtener_mineras_codelco():
    """Obtener lista de mineras que forman parte de CODELCO"""
    return ['MINISTRO HALES', 'RADOMIRO TOMIC', 'CHUQUICAMATA', 'MINA GABY']


def es_minera_codelco(minera_nombre):
    """Verificar si una minera pertenece al grupo CODELCO"""
    return minera_nombre in obtener_mineras_codelco()


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


def obtener_datos_grafico_athena(minera_nombre, fecha_inicio, fecha_fin, viajes_min, viajes_max):
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


def obtener_datos_matriz_athena(minera_nombre, fecha_inicio, fecha_fin, viajes_min, viajes_max):
    df = obtener_datos_desde_athena(minera_nombre, fecha_inicio, fecha_fin)
    if df is None or df.empty:
        return {'transportistas': [], 'fechas': [], 'datos': {}}

    tabla_base, resumen_diario = procesar_datos_athena(df, viajes_min, viajes_max)

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
            datos_trans = tabla_base[(tabla_base['Transportista'] == transportista_nombre) & (tabla_base['Fecha'] == row['Fecha'])]
            viajes_realizados = int(datos_trans['Entregado totalmente'].sum())
            
            # Calcular disponibilidad: (viajes realizados / banda transportista) * 100
            if banda_transportista and banda_transportista > 0:
                disponibilidad = (viajes_realizados / banda_transportista) * 100
                disponibilidad = min(100, disponibilidad)
            else:
                disponibilidad = 0

            matriz_data['datos'][transportista_nombre][fecha_str] = {
                'porcentaje': round(disponibilidad, 1),
                'total': viajes_realizados,
                'banda': banda_transportista,
                'cumplidos': viajes_realizados,
                'transportista_id': 0,
                'fecha_full': row['Fecha'].strftime('%Y-%m-%d')
            }
    
    # Procesar "OTRO TRANSPORTISTA" (medido por entregas, no por banda)
    if not otros_transportistas_data.empty:
        matriz_data['bandas_transportistas']['OTRO TRANSPORTISTA'] = None  # Sin banda
        matriz_data['datos']['OTRO TRANSPORTISTA'] = {}
        
        for _, row in resumen_diario.iterrows():
            fecha_str = row['Fecha'].strftime('%d-%m')
            datos_otros = otros_transportistas_data[otros_transportistas_data['Fecha'] == row['Fecha']]
            entregas_realizadas = int(datos_otros['Entregado totalmente'].sum())
            
            # Para "otros" no usamos porcentaje de banda, solo mostramos entregas
            matriz_data['datos']['OTRO TRANSPORTISTA'][fecha_str] = {
                'porcentaje': 0,  # No aplica concepto de banda
                'total': entregas_realizadas,
                'banda': None,  # Sin banda asignada
                'cumplidos': entregas_realizadas,
                'transportista_id': 0,
                'fecha_full': row['Fecha'].strftime('%Y-%m-%d'),
                'es_otro': True  # Marca especial para el frontend
            }

    return matriz_data


def obtener_transportistas_global():
    """Obtener transportistas únicos de la tabla completa (para administración)."""
    if not USE_ATHENA:
        return []
    query = f"SELECT DISTINCT carriername1 as transportista FROM {DATABASE_ATHENA}.{TABLA_PEDIDOS} WHERE carriername1 IS NOT NULL ORDER BY transportista"
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
    
    if USE_ATHENA and minera_nombre and fecha:
        fecha_obj = datetime.strptime(fecha, '%Y-%m-%d').date()
        
        # Obtener datos completos (SCR + Turnos + RCO)
        datos_completos = obtener_datos_completos_athena(minera_nombre, fecha_obj, fecha_obj)
        
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
                if transportista_nombre:
                    datos_completos_filtrados = datos_completos[datos_completos['Transportista'] == transportista_nombre]
                else:
                    datos_completos_filtrados = datos_completos
                
                # Para mantener compatibilidad con el template existente, 
                # también obtenemos los datos SCR básicos para los registros individuales
                df_scr = obtener_datos_desde_athena(minera_nombre, fecha_obj, fecha_obj)
                if df_scr is not None and not df_scr.empty:
                    if transportista_nombre:
                        df_scr = df_scr[df_scr['Transportista'] == transportista_nombre]
                    registros = df_scr.to_dict('records')
            
            # Calcular bandas por transportista usando todos los transportistas con datos completos
            if not datos_completos_filtrados.empty:
                transportistas_unicos = datos_completos_filtrados['Transportista'].unique()
                banda_total = 0
                bandas_por_transportista = {}
                
                if transportista_nombre == 'OTRO TRANSPORTISTA':
                    # Para OTRO TRANSPORTISTA, no calcular bandas sino contar entregas
                    for transp in transportistas_unicos:
                        bandas_por_transportista[transp] = None  # Sin banda
                    banda_total = None  # No aplica concepto de banda total
                else:
                    # Caso normal: calcular bandas
                    for transp in transportistas_unicos:
                        banda = calcular_banda_transportista(transp, minera_nombre)
                        bandas_por_transportista[transp] = banda
                        if banda:
                            banda_total += banda
                
                # Calcular disponibilidad operacional según nueva métrica
                disponibilidad_operacional = calcular_disponibilidad_operacional(datos_completos_filtrados)
                
                banda_info = {
                    'banda_total': banda_total,
                    'bandas_por_transportista': bandas_por_transportista,
                    'transportistas_unicos': list(transportistas_unicos),
                    'es_otro_transportista': transportista_nombre == 'OTRO TRANSPORTISTA',
                    'disponibilidad_operacional': disponibilidad_operacional
                }
                
                # Usar los datos filtrados para el template
                datos_completos = datos_completos_filtrados

    mineras = obtener_mineras_athena()
    transportistas = obtener_transportistas_global() if USE_ATHENA else []

    return render_template('detalle.html', 
                         minera=minera_nombre, 
                         transportista=transportista_nombre,
                         fecha=fecha,
                         registros=registros,
                         datos_completos=datos_completos.to_dict('records') if datos_completos is not None and not datos_completos.empty else [],
                         banda_info=banda_info,
                         mineras=mineras,
                         transportistas=transportistas)


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