from flask import Flask, render_template, request, jsonify
from datetime import datetime, timedelta
import os
import pandas as pd
import numpy as np

# Intentar importar pyathena, pero permitir que la app funcione sin él
try:
    from pyathena import connect
    from pyathena.pandas.cursor import PandasCursor
    ATHENA_AVAILABLE = True
except ImportError:
    ATHENA_AVAILABLE = False
    print("⚠️ pyathena no disponible. La app usará datos de ejemplo de SQLite.")

app = Flask(__name__)
app.config['SECRET_KEY'] = 'dev-secret-key-change-in-production'

# Configuración de AWS Athena (usar variables de entorno en producción)
AWS_CONFIG = {
    'aws_access_key_id': os.getenv('AWS_ACCESS_KEY', ''),
    'aws_secret_access_key': os.getenv('AWS_SECRET_KEY', ''),
    's3_staging_dir': os.getenv('S3_BUCKET', ''),
    'region_name': os.getenv('AWS_REGION', '')
}

# Nombre base de datos/tabla en Athena
DATABASE_ATHENA = 'logistica_scr_staging'
TABLA_PEDIDOS = 'etlist'


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

    # Escapar comillas simples en el nombre de la minera
    minera_safe = minera_nombre.replace("'", "''")

    # Query adaptada de athena_test.py
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
        'MINISTRO HALES',
        'RADOMIRO TOMIC',
        'CHUQUICAMATA',
        'MINA GABY'
    ]
    return mineras_predefinidas


def obtener_transportistas_athena(minera_nombre, fecha_inicio, fecha_fin):
    """Obtener transportistas únicos para una minera y rango de fechas"""
    df = obtener_datos_desde_athena(minera_nombre, fecha_inicio, fecha_fin)
    if df is None or df.empty:
        return []
    return sorted(df['Transportista'].fillna('SIN_INFORMACION').unique().tolist())


def obtener_datos_grafico_athena(minera_nombre, fecha_inicio, fecha_fin, viajes_min, viajes_max):
    df = obtener_datos_desde_athena(minera_nombre, fecha_inicio, fecha_fin)
    if df is None or df.empty:
        return {'datos': [], 'minimo': viajes_min, 'maximo': viajes_max}

    _, resumen_diario = procesar_datos_athena(df, viajes_min, viajes_max)

    grafico_data = {
        'datos': [
            {
                'fecha': row['Fecha'].strftime('%d-%m'),
                'cantidad': int(row['Entregado totalmente']),
                'cumple': bool(row['En_rango'])
            }
            for _, row in resumen_diario.iterrows()
        ],
        'minimo': viajes_min,
        'maximo': viajes_max
    }

    return grafico_data


def obtener_datos_matriz_athena(minera_nombre, fecha_inicio, fecha_fin, viajes_min, viajes_max):
    df = obtener_datos_desde_athena(minera_nombre, fecha_inicio, fecha_fin)
    if df is None or df.empty:
        return {'transportistas': [], 'fechas': [], 'datos': {}}

    tabla_base, resumen_diario = procesar_datos_athena(df, viajes_min, viajes_max)

    transportistas_unicos = tabla_base['Transportista'].unique()
    fechas = [d.strftime('%d-%m') for d in sorted(resumen_diario['Fecha'].unique())]

    matriz_data = {
        'transportistas': list(transportistas_unicos),
        'fechas': fechas,
        'datos': {}
    }

    for transportista_nombre in transportistas_unicos:
        matriz_data['datos'][transportista_nombre] = {}
        for _, row in resumen_diario.iterrows():
            fecha_str = row['Fecha'].strftime('%d-%m')
            datos_trans = tabla_base[(tabla_base['Transportista'] == transportista_nombre) & (tabla_base['Fecha'] == row['Fecha'])]
            total = int(datos_trans['Entregado totalmente'].sum())
            porcentaje = 100 if total > 0 else 0

            matriz_data['datos'][transportista_nombre][fecha_str] = {
                'porcentaje': int(porcentaje),
                'total': total,
                'cumplidos': total,
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
        return sorted(df['transportista'].dropna().unique().tolist())
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
    viajes_min = request.args.get('viajes_min', type=int, default=11)
    viajes_max = request.args.get('viajes_max', type=int, default=13)

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


@app.route('/api/mineras', methods=['GET', 'POST'])
def api_mineras():
    """Endpoints de mineras — Athena-only read API. POST no soportado."""
    if request.method == 'GET':
        mineras = obtener_mineras_athena()
        # devolver lista simple con valores por defecto para min/max
        return jsonify([{
            'nombre': m,
            'viajes_minimos': 11,
            'viajes_maximos': 13
        } for m in mineras])

    elif request.method == 'POST':
        return jsonify({'error': 'Creación de mineras no soportada en modo Athena-only'}), 405


# Database initialization and local example data removed — Athena-only mode


if __name__ == '__main__':
    # Athena-only mode: ensure USE_ATHENA is True in env or pyathena is installed.
    app.run(debug=True, port=5000)
