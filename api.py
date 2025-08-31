from fastapi import FastAPI, HTTPException
import pandas as pd
from Config.config import Config
from Transform.transformer import Transformer
from Extract.extractor import Extractor

app = FastAPI(title="ETL Data Analysis API", version="1.0")

# Cargar y limpiar los datos al iniciar la API
extractor = Extractor(Config.INPUT_PATH)
raw_df = extractor.extract()

if raw_df is not None:
    transformer = Transformer(raw_df)
    df = transformer.clean()
else:
    df = pd.DataFrame()  # dataframe vacío si falla la extracción

@app.get("/")
def root():
    return {"message": "API ETL funcionando correctamente"}

@app.get("/summary")
def get_summary():
    """
    Devuelve un resumen estadístico de los datos numéricos
    """
    if df.empty:
        raise HTTPException(status_code=404, detail="No hay datos disponibles")
    
    summary = df.describe(include='all').to_dict()
    return summary

@app.get("/filter")
def filter_data(column: str, value: str):
    """
    Filtra los datos por columna y valor
    """
    if column not in df.columns:
        raise HTTPException(status_code=400, detail=f"La columna {column} no existe")
    
    filtered = df[df[column].astype(str).str.contains(value, case=False)]
    
    return filtered.to_dict(orient="records")

@app.get("/stats/{column}")
def column_stats(column: str):
    """
    Devuelve estadísticas básicas (media, suma, min, max) de una columna numérica
    """
    if column not in df.columns:
        raise HTTPException(status_code=400, detail=f"La columna {column} no existe")
    if not pd.api.types.is_numeric_dtype(df[column]):
        raise HTTPException(status_code=400, detail=f"La columna {column} no es numérica")
    
    stats = {
        "mean": df[column].mean(),
        "sum": df[column].sum(),
        "min": df[column].min(),
        "max": df[column].max()
    }
    return stats
