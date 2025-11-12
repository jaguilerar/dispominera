from flask import Flask, render_template, request, jsonify
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
        },
        
        # Bandas por defecto basadas en minera (para transportistas no especificados)
        'default': {
            'MINA LA ESCONDIDA': 5,     # viajes por día por transportista
            'QUADRA SIERRA GORDA': 3,
            'ANDINA': 2,
            'EL TENIENTE': 1,
            'CASERONES': 2,
            'SALARES NORTE': 2,
            'MINERA CANDELARIA': 3,
            'LOS BRONCES': 3,
            'CODELCO': 4
        }
    }
    return bandas_transportista


def calcular_banda_transportista(transportista, minera, bandas_config=None):
    """Calcular la banda (capacidad diaria) de un transportista para una minera específica"""
    if bandas_config is None:
        bandas_config = obtener_configuracion_bandas_transportista()
    
    # Normalizar nombre del transportista para búsqueda
    transportista_normalizado = transportista.upper().strip()
    
    # Buscar configuración específica del transportista (exacta)
    if transportista_normalizado in bandas_config and minera in bandas_config[transportista_normalizado]:
        return bandas_config[transportista_normalizado][minera]
    
    # Buscar por patrones parciales (para manejo de variaciones de nombres)
    for nombre_config in bandas_config:
        if nombre_config != 'default':
            # Buscar si el nombre configurado está contenido en el nombre del transportista
            if nombre_config.upper() in transportista_normalizado or transportista_normalizado in nombre_config.upper():
                if minera in bandas_config[nombre_config]:
                    return bandas_config[nombre_config][minera]
    
    # Usar banda por defecto de la minera
    if minera in bandas_config['default']:
        return bandas_config['default'][minera]
    
    # Banda por defecto general
    return 3


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

    # Obtener transportistas únicos
    transportistas_unicos = sorted(tabla_base['Transportista'].unique())
    
    # Crear estructura para gráfico apilado
    fechas_ordenadas = sorted(tabla_base['Fecha'].unique())
    
    datos_apilados = []
    for fecha in fechas_ordenadas:
        fecha_str = fecha.strftime('%d-%m')
        datos_fecha = {'fecha': fecha_str, 'total': 0}
        
        # Agregar datos por transportista para esta fecha
        for transportista in transportistas_unicos:
            datos_transportista = tabla_base[
                (tabla_base['Fecha'] == fecha) & 
                (tabla_base['Transportista'] == transportista)
            ]
            cantidad = int(datos_transportista['Entregado totalmente'].sum()) if not datos_transportista.empty else 0
            datos_fecha[transportista] = cantidad
            datos_fecha['total'] += cantidad
        
        # Verificar si cumple con el rango
        datos_fecha['cumple'] = viajes_min <= datos_fecha['total'] <= viajes_max
        datos_apilados.append(datos_fecha)

    grafico_data = {
        'datos': datos_apilados,
        'minimo': viajes_min,
        'maximo': viajes_max,
        'tipo': 'apilado',
        'transportistas': transportistas_unicos
    }

    return grafico_data


def obtener_datos_matriz_athena(minera_nombre, fecha_inicio, fecha_fin, viajes_min, viajes_max):
    df = obtener_datos_desde_athena(minera_nombre, fecha_inicio, fecha_fin)
    if df is None or df.empty:
        return {'transportistas': [], 'fechas': [], 'datos': {}}

    tabla_base, resumen_diario = procesar_datos_athena(df, viajes_min, viajes_max)

    transportistas_unicos = tabla_base['Transportista'].unique()
    fechas = [d.strftime('%d-%m') for d in sorted(resumen_diario['Fecha'].unique())]

    # Obtener configuración de bandas por transportista
    bandas_config = obtener_configuracion_bandas_transportista()

    matriz_data = {
        'transportistas': list(transportistas_unicos),
        'fechas': fechas,
        'datos': {},
        'bandas_transportistas': {}  # Para referencia en el frontend
    }

    for transportista_nombre in transportistas_unicos:
        # Obtener banda específica del transportista para esta minera
        banda_transportista = calcular_banda_transportista(transportista_nombre, minera_nombre, bandas_config)
        matriz_data['bandas_transportistas'][transportista_nombre] = banda_transportista
        
        matriz_data['datos'][transportista_nombre] = {}
        for _, row in resumen_diario.iterrows():
            fecha_str = row['Fecha'].strftime('%d-%m')
            datos_trans = tabla_base[(tabla_base['Transportista'] == transportista_nombre) & (tabla_base['Fecha'] == row['Fecha'])]
            viajes_realizados = int(datos_trans['Entregado totalmente'].sum())
            
            # Calcular disponibilidad: (viajes realizados / banda transportista) * 100
            if banda_transportista > 0:
                disponibilidad = (viajes_realizados / banda_transportista) * 100
                # Limitar al 100% máximo para mejor visualización
                disponibilidad = min(100, disponibilidad)
            else:
                disponibilidad = 0

            matriz_data['datos'][transportista_nombre][fecha_str] = {
                'porcentaje': round(disponibilidad, 1),  # Disponibilidad como %
                'total': viajes_realizados,             # Viajes realizados
                'banda': banda_transportista,           # Capacidad del transportista
                'cumplidos': viajes_realizados,         # Compatibilidad con frontend
                'transportista_id': 0,
                'fecha_full': row['Fecha'].strftime('%Y-%m-%d')
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
def index():
    """Vista principal - Dashboard (lista de mineras predefinidas)"""
    mineras = obtener_mineras_athena()
    data_source = 'Lista predefinida de mineras'
    return render_template('index.html', mineras=mineras, data_source=data_source)


@app.route('/detalle')
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
    if USE_ATHENA and minera_nombre and fecha:
        fecha_obj = datetime.strptime(fecha, '%Y-%m-%d').date()
        # obtener datos desde Athena y filtrar
        df = obtener_datos_desde_athena(minera_nombre, fecha_obj, fecha_obj)
        if df is not None and not df.empty:
            if transportista_nombre:
                df = df[df['Transportista'] == transportista_nombre]

            # Mapear filas a objetos simples para la plantilla
            registros = df.to_dict('records')

    mineras = obtener_mineras_athena()
    transportistas = obtener_transportistas_global() if USE_ATHENA else []

    return render_template('detalle.html', 
                         minera=minera_nombre, 
                         transportista=transportista_nombre,
                         fecha=fecha,
                         registros=registros,
                         mineras=mineras,
                         transportistas=transportistas)


@app.route('/api/dashboard_data')
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
def admin_mineras():
    """Gestión de mineras y asociaciones"""
    mineras = obtener_mineras_athena()
    transportistas = obtener_transportistas_global() if USE_ATHENA else []
    return render_template('admin_mineras.html', mineras=mineras, transportistas=transportistas)


@app.route('/api/debug/transportistas')
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
    
    app.run(host='0.0.0.0', port=port, debug=debug)
