class Transformer:
    """
    Clase para transformar y limpiar los datos extraídos de Steam Games.
    """
    def __init__(self, df):
        self.df = df

    def clean(self):
        """
        Realiza limpieza y transformación de los datos de Steam Games.
        """
        import pandas as pd
        df = self.df.copy()
        
        # Eliminar filas sin nombre o URL (campos requeridos)
        df = df.dropna(subset=['name', 'url'])
        
        # Limpiar URLs duplicadas
        df = df.drop_duplicates(subset=['url'])
        
        # Limpiar y normalizar columnas de texto
        text_cols = ['name', 'desc_snippet', 'developer', 'publisher', 'genre', 'popular_tags']
        for col in text_cols:
            if col in df.columns:
                df[col] = df[col].fillna('Unknown')
                df[col] = df[col].astype(str).str.strip()
        
        # Limpiar columnas de precios
        price_cols = ['original_price', 'discount_price']
        for col in price_cols:
            if col in df.columns:
                # Remove currency symbols and convert to numeric
                df[col] = df[col].astype(str).str.replace('$', '').str.replace(',', '')
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)
        
        # Limpiar achievements (convertir a entero)
        if 'achievements' in df.columns:
            df['achievements'] = pd.to_numeric(df['achievements'], errors='coerce').fillna(0).astype(int)
        
        # Limpiar fechas de lanzamiento
        if 'release_date' in df.columns:
            df['release_date'] = df['release_date'].fillna('Unknown')
        
        # Limpiar reviews
        review_cols = ['recent_reviews', 'all_reviews']
        for col in review_cols:
            if col in df.columns:
                df[col] = df[col].fillna('No reviews')
        
        # Limpiar requirements
        req_cols = ['minimum_requirements', 'recommended_requirements']
        for col in req_cols:
            if col in df.columns:
                df[col] = df[col].fillna('Not specified')
        
        # Limpiar game_details y languages
        if 'game_details' in df.columns:
            df['game_details'] = df['game_details'].fillna('No details')
        
        if 'languages' in df.columns:
            df['languages'] = df['languages'].fillna('English')
        
        # Limpiar mature_content
        if 'mature_content' in df.columns:
            df['mature_content'] = df['mature_content'].fillna('Not specified')
        
        # Limpiar game_description
        if 'game_description' in df.columns:
            df['game_description'] = df['game_description'].fillna('No description available')
        
        # Limpiar types
        if 'types' in df.columns:
            df['types'] = df['types'].fillna('app')
        
        print(f"Datos limpiados: {len(df)} registros después de la transformación")
        
        self.df = df
        return self.df
