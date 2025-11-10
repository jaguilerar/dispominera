# DispoMinera - Sistema de Monitoreo de Conformidad del Transporte

Aplicación web desarrollada en Flask para el monitoreo y análisis del cumplimiento del transporte minero, integrada con AWS Athena para datos en tiempo real.

## 📋 Características

- **Dashboard Interactivo**: Visualización de entregas por día con gráficos dinámicos
- **Integración con AWS Athena**: Conexión directa a la tabla `historial_turnos` en `dispomate_staging`
- **Matriz de Cumplimiento**: Seguimiento por transportista y fecha con indicadores visuales
- **Vista de Detalle**: Registro detallado de viajes con información completa
- **Modo Dual**: Funciona con datos de Athena o SQLite local
- **Parametrización**: Configuración flexible de viajes mínimos/máximos por minera

## 🔄 Modos de Operación

### Modo 1: SQLite Local (Por defecto)
- Usa base de datos local con datos de ejemplo
- No requiere credenciales de AWS
- Ideal para desarrollo y pruebas
- Se activa automáticamente si `USE_ATHENA=false`

### Modo 2: AWS Athena (Producción)
- Conecta a Athena para datos reales
- Requiere credenciales de AWS
- Accede a `dispomate_staging.historial_turnos`
- Se activa con `USE_ATHENA=true`

## 🚀 Instalación

### Prerrequisitos

- Python 3.8 o superior
- pip (gestor de paquetes de Python)
- (Opcional) Credenciales de AWS para acceso a Athena

### Pasos de Instalación

1. **Descomprimir el proyecto**

2. **Crear entorno virtual** (recomendado):
```bash
python -m venv venv
```

3. **Activar entorno virtual**:
   - Windows:
     ```bash
     venv\Scripts\activate
     ```
   - Linux/Mac:
     ```bash
     source venv/bin/activate
     ```

4. **Instalar dependencias**:
```bash
pip install -r requirements.txt
```

5. **Configurar credenciales** (opcional, solo para Athena):
```bash
# Copiar archivo de ejemplo
cp .env.example .env

# Editar .env y completar credenciales
# Cambiar USE_ATHENA=true
```

## 🎯 Uso

### Modo SQLite (Por defecto)

```bash
python app.py
```

La aplicación estará disponible en: `http://localhost:5000`

### Modo Athena

1. **Configurar variables de entorno**:

Editar `.env`:
```bash
USE_ATHENA=true
AWS_ACCESS_KEY=tu_access_key
AWS_SECRET_KEY=tu_secret_key
S3_BUCKET=s3://copec-gobierno-athena-queries-prd/copec/jaguilera@copec.cl
AWS_REGION=us-east-1
```

2. **Iniciar aplicación**:
```bash
python app.py
```

La app mostrará en consola si está usando Athena o SQLite.

## 📊 Estructura de Datos

### Athena: Tabla historial_turnos

Campos utilizados:
- `vbeln`: Número de orden
- `name1kunag`: Cliente (Minera)
- `carriername1`: Transportista
- `vehtext`: Camión
- `fechasalidaprog`: Fecha programada
- `statproc`: Estado del proceso
- `especial`: Marca si es licitado

### SQLite: Base de datos local

Tablas:
- `minera`: Configuración de mineras
- `transportista`: Catálogo de transportistas
- `registro_viaje`: Registros de viajes
- `asociacion_minera_transportista`: Relaciones

## 🔧 Configuración

### Variables de Entorno

```bash
# Activar/desactivar Athena
USE_ATHENA=true|false

# Credenciales AWS (solo si USE_ATHENA=true)
AWS_ACCESS_KEY=...
AWS_SECRET_KEY=...
S3_BUCKET=...
AWS_REGION=us-east-1
```

### Parámetros de Minera

Cada minera configura:
- **viajes_minimos_esperados**: Umbral mínimo de viajes por día
- **viajes_maximos_esperados**: Umbral máximo de viajes por día

## 📱 Estructura de la Aplicación

### Páginas Principales

1. **Dashboard (`/`)**
   - Fuente de datos indicada en pantalla (Athena/SQLite)
   - Filtros por Minera, Mes y Semana
   - Gráfico de entregas totales por día
   - Matriz de cumplimiento por transportista

2. **Vista de Detalle (`/detalle`)**
   - Tabla completa de registros de viajes
   - Filtros por Minera, Transportista y Fecha
   - Resumen de cumplimiento

3. **Administración (`/admin/mineras`)**
   - Gestión de mineras
   - Gestión de transportistas
   - Configuración de asociaciones

## 🔐 Seguridad

### ⚠️ IMPORTANTE

1. **NUNCA** incluyas credenciales en el código
2. **SIEMPRE** usa variables de entorno
3. Agrega `.env` a tu `.gitignore`
4. En producción, usa AWS IAM roles en lugar de access keys
5. Las credenciales en el código de ejemplo deben ser reemplazadas

### Buenas Prácticas

```bash
# Crear .gitignore
echo ".env" >> .gitignore
echo "*.db" >> .gitignore
echo "__pycache__/" >> .gitignore
```

## 📊 Lógica de Procesamiento

### Desde Athena

La aplicación replica la lógica del notebook original:

1. **Extracción**: Query a `historial_turnos` filtrado por minera y fechas
2. **Transformación**:
   - Calcula "Entregado totalmente" desde `statproc`
   - Identifica licitados/no licitados desde `especial`
   - Agrupa por fecha y transportista
3. **Agregación**:
   - Resumen diario de entregas
   - Porcentaje de cumplimiento por transportista
   - Validación contra umbrales mín/máx

### Desde SQLite

Usa la misma lógica pero con datos locales estructurados.

## 🛠️ Tecnologías Utilizadas

- **Backend**: Flask, SQLAlchemy
- **Frontend**: HTML5, CSS3, JavaScript
- **Gráficos**: Chart.js
- **Base de Datos**: SQLite (dev), Athena (prod)
- **AWS**: PyAthena para conexión a Athena
- **Análisis**: Pandas, NumPy

## 🚨 Troubleshooting

### Error: "ModuleNotFoundError: No module named 'pyathena'"

**Solución**:
```bash
pip install PyAthena
```

### Error: "Unable to locate credentials"

**Solución**: Verifica tu archivo `.env`:
```bash
# Asegúrate que .env existe y tiene:
USE_ATHENA=true
AWS_ACCESS_KEY=tu_access_key
AWS_SECRET_KEY=tu_secret_key
```

### La app no usa Athena

**Verificar**:
1. `USE_ATHENA=true` en `.env`
2. PyAthena instalado
3. Credenciales correctas
4. Revisar logs en consola al iniciar

### Query de Athena falla

**Posibles causas**:
1. Nombre de base de datos incorrecto
2. Nombre de tabla incorrecto
3. Permisos IAM insuficientes
4. Formato de fechas en query

## 📝 Próximas Funcionalidades

- [ ] Cache de queries de Athena
- [ ] Exportación de datos a Excel
- [ ] Carga masiva de datos
- [ ] Dashboard de KPIs adicionales
- [ ] Alertas automáticas por incumplimiento

## 📄 Archivos Importantes

```
dispominera_v2/
├── app.py                 # Aplicación principal
├── requirements.txt       # Dependencias
├── .env.example          # Plantilla de configuración
├── templates/            # Templates HTML
│   ├── base.html
│   ├── index.html
│   ├── detalle.html
│   └── admin_mineras.html
└── README.md             # Este archivo
```

## 🎓 Integración con Notebook Original

Esta aplicación web está basada en el notebook `dispominera.ipynb` y mantiene la misma lógica:

- ✅ Conexión a Athena con PyAthena
- ✅ Query a `dispomate_staging.historial_turnos`
- ✅ Procesamiento de datos con Pandas
- ✅ Cálculo de "Entregado totalmente"
- ✅ Distinción licitados/no licitados
- ✅ Umbrales configurables de viajes

## 📞 Soporte

Para problemas:
1. Revisa los logs en consola
2. Verifica configuración en `.env`
3. Confirma credenciales de AWS
4. Revisa documentación de PyAthena

---

**Desarrollado para**: Copec - Planning  
**Versión**: 2.0  
**Fecha**: Noviembre 2025
