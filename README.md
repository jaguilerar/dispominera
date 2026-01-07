# DispoMinera - Sistema de Monitoreo de Conformidad del Transporte

Aplicación web desarrollada en Flask para el monitoreo y análisis del cumplimiento del transporte minero, integrada con AWS Athena para datos en tiempo real.

## 📋 Características

- **Dashboard Interactivo**: Visualización de entregas por día con gráficos dinámicos
- **Integración con AWS Athena**: Conexión directa a la base de datos `logistica_scr_staging.etlist`
- **Sistema de Caché Optimizado** ⚡: Reduce tiempos de carga hasta 90% y costos de Athena
- **Queries SQL Optimizadas**: Selección inteligente de columnas y filtrado en origen
- **Matriz de Cumplimiento**: Seguimiento por transportista y fecha con indicadores visuales
- **Vista de Detalle**: Registro detallado de viajes con información completa
- **Selector de Período**: Filtros por Año, Mes y Semana para análisis temporal flexible
- **Lista Predefinida de Mineras**: 12 mineras principales precargadas en el sistema
- **Loading Screen Inteligente**: Feedback visual durante la carga de datos con spinner animado
- **Manejo de Errores Elegante**: Pantallas de error informativas y contextual

## ⚡ Mejoras de Rendimiento (NUEVO)

El sistema ahora incluye optimizaciones avanzadas:

- **Caché en memoria/Redis**: Almacena resultados de queries costosas
- **Reducción de tiempo de carga**: De 8-15s a 0.5-2s en cargas subsecuentes
- **Ahorro de costos AWS**: Hasta 90% menos queries a Athena
- **Queries optimizadas**: Solo se obtienen columnas necesarias
- **API de gestión de caché**: Endpoints para limpiar y monitorear caché

📖 Ver [OPTIMIZACION_CACHE.md](./OPTIMIZACION_CACHE.md) para detalles completos.

## 🏭 Mineras Soportadas

La aplicación incluye las siguientes mineras predefinidas:

- **MINA LA ESCONDIDA**
- **QUADRA SIERRA GORDA**
- **ANDINA**
- **EL TENIENTE**
- **CASERONES**
- **SALARES NORTE**
- **MINERA CANDELARIA**
- **LOS BRONCES**
- **MINISTRO HALES**
- **RADOMIRO TOMIC**
- **CHUQUICAMATA**
- **MINA GABY**

## 🔄 Modos de Operación

### Modo Athena (Principal)
- Conecta a AWS Athena para datos reales
- Accede a `logistica_scr_staging.etlist`
- Requiere credenciales de AWS configuradas
- Se activa con `USE_ATHENA=true` (por defecto)

### Modo Local (Fallback)
- Se activa automáticamente si Athena no está disponible
- Muestra interfaz completa sin datos
- Útil para desarrollo y pruebas de UI
- No requiere credenciales de AWS

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

### Configuración Rápida

```bash
# Instalar dependencias
pip install -r requirements.txt

# Ejecutar la aplicación
python app.py
```

La aplicación estará disponible en: `http://localhost:5000`

### Con Athena (Recomendado)

1. **Configurar variables de entorno**:

Crear archivo `.env`:
```bash
USE_ATHENA=true
AWS_ACCESS_KEY=tu_access_key
AWS_SECRET_KEY=tu_secret_key
S3_BUCKET=s3://tu-bucket-athena-queries/
AWS_REGION=us-east-1
```

2. **Iniciar aplicación**:
```bash
python app.py
```

La app mostrará en consola si está usando Athena o modo local.

## 📊 Interfaz de Usuario

### 🎛️ Controles de Filtros

La aplicación incluye filtros intuitivos:

1. **Selector de Minera**: Dropdown con las 12 mineras predefinidas
2. **Selector de Año**: Opciones 2023, 2024, 2025 (2025 por defecto)
3. **Selector de Mes**: Enero a Diciembre (Octubre por defecto)
4. **Selector de Semana**: Se actualiza automáticamente según el mes y año seleccionados

### 📈 Dashboard Principal

- **Gráfico de Barras**: Entregas totalmente por día con códigos de color
  - 🟢 Verde: Cumple con objetivos (entre 11-13 viajes)
  - 🔴 Rojo: No cumple objetivos
- **Matriz de Cumplimiento**: Vista por transportista y fecha
  - Círculos de porcentaje clickeables
  - Navegación directa a vista de detalle

### 🔄 Estados de Carga

La aplicación incluye un sistema de loading inteligente:

- **🎯 Spinner Animado**: Indicador visual durante consultas
- **📝 Mensajes Contextuales**: 
  - "Consultando base de datos..."
  - "Procesando datos... Generando gráficos y matrices"
- **⚠️ Manejo de Errores**: Pantallas informativas con detalles del error
- **📊 Carga por Fases**: Feedback específico para cada operación

## 📊 Estructura de Datos

### Athena: Tabla `logistica_scr_staging.etlist`

Campos utilizados:
- `vdatu`: Fecha de la entrega
- `vtext`: Cliente/Minera
- `carriername1`: Nombre del transportista
- `descrstatu`: Estado de la entrega ('Entregado totalmente', 'Recibí Conforme')
- `vbeln`: Número de pedido
- Otros campos adicionales para análisis detallado

### Lógica de Procesamiento

1. **Filtrado**: Por minera, rango de fechas y estado de entrega
2. **Agregación**: 
   - Entregas por día y transportista
   - Resumen diario total
   - Cálculo de cumplimiento vs objetivos (11-13 viajes/día)
3. **Validación**: Comparación contra umbrales configurables

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
   - Lista predefinida de 12 mineras principales
   - Filtros por Año (2023-2025), Mes y Semana
   - Gráfico de entregas totales por día con códigos de color
   - Matriz de cumplimiento por transportista con porcentajes clickeables
   - Sistema de loading con spinner animado

2. **Vista de Detalle (`/detalle`)**
   - Tabla completa de registros de viajes
   - Filtros por Minera, Transportista y Fecha
   - Navegación desde matriz de cumplimiento
   - Resumen de entregas por registro

3. **API Endpoints**
   - `/api/dashboard_data`: Datos principales con soporte para filtros de año
   - `/api/semanas/<mes>`: Cálculo dinámico de semanas por mes y año

¡Excelente idea! Voy a implementar un loading screen que se muestre mientras se cargan los datos. Agregaré un spinner visual y actualizaré los mensajes de estado.

1. Primero agrego el CSS para el spinner en el head o en un archivo CSS. Voy a revisar la estructura del proyecto:
Read base.html

Perfecto, veo que ya hay estilos para .loading. Ahora voy a agregar CSS para el spinner y actualizar el JavaScript para mostrar estados de carga más elegantes.

1. Primero agrego CSS para el spinner en base.html:
base.html+36-0
2. Ahora actualizo el JavaScript para mostrar el loading spinner. Primero creo una función para mostrar el loading:
index.html+16-0
3. Actualizo la función loadSemanas para mostrar loading:
index.html+10-1
4. Actualizo loadDashboardData para mostrar loading detallado:
index.html+36-1
5. Ahora agrego loading al gráfico también, actualizando updateChart:
index.html+30-0
¡Perfecto! 🎉 He implementado un sistema completo de loading con las siguientes características:
Nuevas características del Loading Screen:
🎨 Spinner animado: Un elegante spinner rojo que coincide con los colores de tu app
📝 Mensajes contextuales: Diferentes mensajes según el estado de carga
📊 Detalles específicos: Muestra qué minera y período se está consultando
⚠️ Manejo de errores: Pantalla de error elegante con detalles específicos
🔄 Estados múltiples: Loading para semanas y datos principales
Estados de Loading implementados:
Carga de semanas:
Select deshabilitado temporalmente
Mensaje "Cargando semanas..."
Carga de datos principales:
Fase 1: "Consultando base de datos..." + detalles de la consulta
Fase 2: "Procesando datos..." + "Generando gráficos y matrices"
Estado de error:
Ícono de advertencia
Mensaje de error claro
Detalles técnicos del error
Sugerencia de solución
Mejoras visuales:
🎯 Spinner: 50px, animación suave de 1 segundo, color 
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

### La app no conecta a Athena

**Verificar**:
1. `USE_ATHENA=true` en `.env`
2. PyAthena instalado: `pip install PyAthena`
3. Credenciales AWS correctas
4. Revisar logs en consola al iniciar

### Loading infinito o datos no cargan

**Posibles causas**:
1. Credenciales de AWS incorrectas o expiradas
2. Permisos insuficientes en IAM
3. Nombre incorrecto de base de datos o tabla
4. Red/proxy corporativo bloqueando conexión AWS

**Solución**:
```bash
# Verificar credenciales
python -c "from pyathena import connect; print('OK')"

# Verificar variables de entorno
python -c "import os; print(f'Athena: {os.getenv(\"USE_ATHENA\")}')"
```

### Error en consultas de fechas

**Verificar**:
1. Formato de fechas en Athena (YYYY-MM-DD)
2. Zona horaria correcta
3. Datos disponibles para el período seleccionado

### Interface loading pero sin datos

**Causa común**: No hay datos en Athena para la combinación Minera/Fecha seleccionada.

**Solución**: Verificar disponibilidad de datos en el período consultado.

## 📝 Próximas Funcionalidades

- [ ] Cache de queries de Athena para mejor performance
- [ ] Exportación de datos a Excel/PDF
- [ ] Dashboard de KPIs adicionales y métricas avanzadas
- [ ] Alertas automáticas por incumplimiento vía email
- [ ] Filtros adicionales (por transportista, tipo de carga)
- [ ] Histórico de tendencias por minera
- [ ] Comparación entre períodos
- [ ] API REST completa para integración externa

## 🎨 Características Técnicas

### Frontend
- **Responsive Design**: Se adapta a móviles y tablets
- **Charts.js**: Gráficos interactivos y animados
- **Loading States**: Spinner animado con mensajes contextuales
- **Error Handling**: Pantallas de error elegantes
- **Color Coding**: Verde/Rojo para cumplimiento/incumplimiento

### Backend
- **Flask**: Micro-framework web de Python
- **PyAthena**: Conexión nativa a AWS Athena
- **Pandas**: Procesamiento eficiente de datos
- **Error Resilience**: Manejo robusto de errores de conexión

### Performance
- **Queries Optimizadas**: Filtros eficientes en Athena
- **Lazy Loading**: Carga de datos bajo demanda
- **Cálculo Dinámico**: Semanas calculadas automáticamente
- **Estado Persistente**: Mantiene selecciones durante navegación

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
- ✅ Query a `logistica_scr_staging.etlist`
- ✅ Procesamiento de datos con Pandas
- ✅ Cálculo de "Entregado totalmente" desde `descrstatu`
- ✅ Agregación por fecha y transportista
- ✅ Umbrales configurables (11-13 viajes por día)
- ✅ Matriz de cumplimiento visual
- ✅ Integración con análisis temporal (año/mes/semana)

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
