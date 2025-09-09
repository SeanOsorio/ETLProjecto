# ETL Project with Database Integration

Este proyecto ETL (Extract, Transform, Load) procesa datos de reservas de viajes con integración completa de base de datos SQLite.

## Estructura del Proyecto

```
ETLProjecto/
├── main.py                     # Punto de entrada principal del ETL
├── requirements.txt            # Dependencias del proyecto
├── Config/
│   └── config.py              # Configuraciones globales
├── Extract/
│   ├── extractor.py          # Módulo de extracción de datos
│   └── Files/                # Archivos de datos
│       ├── ncr_ride_bookings.csv
│       ├── output_clean.csv
│       ├── Uber.pbix
│       └── ...
├── Transform/
│   └── transformer.py        # Módulo de transformación de datos
├── Load/
│   └── loader.py             # Módulo de carga de datos (CSV y DB)
└── Database/
    ├── db_creator.py         # Creador e inicializador de DB
    ├── db_manager.py         # Utilidades de gestión de DB
    └── ride_bookings.db      # Base de datos SQLite (generada)
```

## Nuevas Características

### 1. Base de Datos SQLite Integrada
- **Creación automática** de base de datos y tablas
- **Esquema optimizado** para datos de viajes
- **Índices** para mejor rendimiento de consultas
- **Logging de procesos ETL** para auditoría

### 2. Tablas de la Base de Datos
- `rides`: Datos principales de viajes
- `locations`: Información de ubicaciones
- `payment_types`: Tipos de pago (datos de referencia)
- `etl_logs`: Logs de procesos ETL para auditoría

### 3. Funcionalidades Mejoradas
- **Procesamiento en lotes** para mejor rendimiento
- **Manejo de errores** robusto con logging detallado
- **Gestión de duplicados** (INSERT OR IGNORE)
- **Utilidades de inspección** de base de datos

## Instalación y Configuración

1. **Instalar dependencias**:
```bash
pip install pandas numpy matplotlib seaborn
```

2. **Verificar estructura de archivos**:
Asegúrate de que el archivo `ncr_ride_bookings.csv` esté en `Extract/Files/`

## Uso del Sistema

### Ejecutar el proceso ETL completo
```bash
python main.py
```

Este comando ejecutará todo el proceso:
1. **Inicializar** base de datos (si no existe)
2. **Extraer** datos del archivo CSV
3. **Transformar** y limpiar los datos
4. **Cargar** datos a CSV y base de datos

### Inspeccionar la base de datos
```bash
python Database/db_manager.py
```

### Resetear la base de datos
```python
from Database.db_manager import DatabaseManager
db_manager = DatabaseManager()
db_manager.reset_database()
```

## Configuración

El archivo `Config/config.py` contiene todas las configuraciones:

```python
# Rutas de archivos
INPUT_PATH = 'Extract/Files/ncr_ride_bookings.csv'
SQLITE_DB_PATH = 'Database/ride_bookings.db'

# Configuración de procesamiento
BATCH_SIZE = 1000
MAX_RETRIES = 3
```

## Esquema de Base de Datos

### Tabla `rides`
```sql
CREATE TABLE rides (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ride_id TEXT UNIQUE NOT NULL,
    pickup_datetime TEXT NOT NULL,
    pickup_locationid INTEGER,
    dropoff_locationid INTEGER,
    passenger_count INTEGER,
    trip_distance REAL,
    fare_amount REAL,
    extra REAL,
    mta_tax REAL,
    tip_amount REAL,
    tolls_amount REAL,
    total_amount REAL,
    payment_type INTEGER,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
```

### Tabla `etl_logs`
```sql
CREATE TABLE etl_logs (
    log_id INTEGER PRIMARY KEY AUTOINCREMENT,
    process_name TEXT NOT NULL,
    start_time DATETIME NOT NULL,
    end_time DATETIME,
    status TEXT CHECK(status IN ('STARTED', 'COMPLETED', 'FAILED')),
    records_processed INTEGER DEFAULT 0,
    error_message TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
```

## Características Avanzadas

### 1. Logging y Auditoría
- Todos los procesos ETL se registran en `etl_logs`
- Logs detallados en archivo `etl_process.log`
- Tracking de registros procesados y errores

### 2. Manejo de Errores
- Validación de datos antes de inserción
- Reintentos automáticos en caso de fallos
- Rollback en caso de errores críticos

### 3. Performance
- Procesamiento en lotes para grandes volúmenes
- Índices optimizados para consultas frecuentes
- Mapeo eficiente de esquemas

## Utilidades de Base de Datos

### Inspeccionar resumen completo
```python
from Database.db_manager import print_database_summary
print_database_summary()
```

### Exportar tabla a CSV
```python
from Database.db_manager import DatabaseManager
db_manager = DatabaseManager()
db_manager.export_table_to_csv('rides', 'exported_rides.csv')
```

### Obtener logs de ETL
```python
db_manager = DatabaseManager()
logs = db_manager.get_etl_logs(limit=5)
for log in logs:
    print(f"Process: {log['process_name']} - Status: {log['status']}")
```

## Dataset Original

Este proyecto utiliza el dataset de Uber disponible en Kaggle: [Uber Ride Analytics Dashboard](https://www.kaggle.com/datasets/yashdevladdha/uber-ride-analytics-dashboard)

### Columnas principales procesadas:
- Date/Time: Información temporal de reservas
- Booking ID: Identificador único de reservas
- Customer/Driver info: Datos de usuarios y conductores
- Location data: Información de origen y destino
- Ride metrics: Distancia, tarifas, calificaciones
- Payment info: Métodos de pago y montos

## Próximas Mejoras

- [ ] Soporte para múltiples fuentes de datos
- [ ] Dashboard web para visualización
- [ ] API REST para consulta de datos
- [ ] Procesamiento incremental
- [ ] Integración con sistemas de monitoreo

## Solución de Problemas

### Error: "Import pandas could not be resolved"
```bash
pip install pandas
```

### Error: "Database locked"
Asegúrate de que no haya conexiones abiertas a la base de datos.

### Error: "File not found"
Verifica que el archivo CSV esté en la ruta correcta especificada en `config.py`.

## Contribución

1. Fork el proyecto
2. Crea una rama para tu feature (`git checkout -b feature/nueva-funcionalidad`)
3. Commit tus cambios (`git commit -am 'Agregar nueva funcionalidad'`)
4. Push a la rama (`git push origin feature/nueva-funcionalidad`)
5. Crea un Pull Request
