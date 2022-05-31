def crear_tablas():
    return """
        
        CREATE TABLE IF NOT EXISTS ubicacion(
            Codigo_Ubicacion SERIAL PRIMARY KEY,
            Codigo_Departamento INT,
            Departamento VARCHAR(50),
            Codigo_Entidad INT,
            Entidad VARCHAR(50)
        );
        
        ALTER TABLE ubicacion
        ADD CONSTRAINT UC_Ubicacion UNIQUE (Codigo_Departamento, Departamento, Codigo_Entidad, Entidad);

        CREATE TABLE IF NOT EXISTS periodo(
            Codigo_Periodo VARCHAR(50) PRIMARY KEY,
            Año INT,
            Mes INT
        );

        ALTER TABLE periodo
        ADD CONSTRAINT UC_Periodo UNIQUE (Año, Mes);

        CREATE TABLE IF NOT EXISTS demografia_poblacion(
            Dimension_Demografia_Poblacion VARCHAR(150) PRIMARY KEY,
            Codigo_Ubicacion INT REFERENCES ubicacion (Codigo_Ubicacion),
            Codigo_Periodo VARCHAR(50) REFERENCES periodo (Periodo_Key),
            Subcategoria_Demografia_Poblacion VARCHAR(150),
            Indicador VARCHAR(150),
            Dato_Numerico INT,
            Dato_Cualitativo VARCHAR(150),
            Fuente_Demografia_Poblacion VARCHAR(150),
            Unidad_Medida VARCHAR(150)
        );

        ALTER TABLE demografia_poblacion
        ADD CONSTRAINT UC_Demografia_Poblacion UNIQUE (Codigo_Ubicacion, Codigo_Periodo);

        CREATE TABLE IF NOT EXISTS vivienda_servicios(
            Dimension_Vivienda_Servicios VARCHAR(150) PRIMARY KEY,
            Codigo_Ubicacion INT REFERENCES ubicacion (Codigo_Ubicacion),
            Codigo_Periodo VARCHAR(50) REFERENCES periodo (Periodo_Key),
            Subcategoria_Vivienda_Servicios VARCHAR(150),
            Indicador VARCHAR(150),
            Dato_Numerico INT,
            Dato_Cualitativo VARCHAR(150),
            Fuente_Vivienda_Servicios VARCHAR(150),
            Unidad_Medida VARCHAR(150)
        );

        ALTER TABLE vivienda_servicios
        ADD CONSTRAINT UC_Vivienda_Servicios UNIQUE (Codigo_Ubicacion, Codigo_Periodo);

        CREATE TABLE IF NOT EXISTS educacion(
            Dimension_Educacion VARCHAR(150) PRIMARY KEY,
            Codigo_Ubicacion INT REFERENCES ubicacion (Codigo_Ubicacion),
            Codigo_Periodo VARCHAR(50) REFERENCES periodo (Periodo_Key),
            Subcategoria_Educacion VARCHAR(150),
            Indicador VARCHAR(150),
            Dato_Numerico INT,
            Dato_Cualitativo VARCHAR(150),
            Fuente_Educacion VARCHAR(150),
            Unidad_Medida VARCHAR(150)
        );

        ALTER TABLE educacion
        ADD CONSTRAINT UC_Educacion UNIQUE (Codigo_Ubicacion, Codigo_Periodo);

        CREATE TABLE IF NOT EXISTS salud(
            Dimension_Salud VARCHAR(150) PRIMARY KEY,
            Codigo_Ubicacion INT REFERENCES ubicacion (Codigo_Ubicacion),
            Codigo_Periodo VARCHAR(50) REFERENCES periodo (Periodo_Key),
            Subcategoria_Salud VARCHAR(150),
            Indicador VARCHAR(150),
            Dato_Numerico INT,
            Dato_Cualitativo VARCHAR(150),
            Fuente_Salud VARCHAR(150),
            Unidad_Medida VARCHAR(150)
        );

        ALTER TABLE salud
        ADD CONSTRAINT UC_Salud UNIQUE (Codigo_Ubicacion, Codigo_Periodo);

        CREATE TABLE IF NOT EXISTS fact_table(
            Fact_Key INT PRIMARY KEY,
            Codigo_Ubicacion INT REFERENCES ubicacion (Codigo_Ubicacion),
            Codigo_Periodo VARCHAR(50) REFERENCES periodo (Codigo_Periodo),
            Dimension_Educacion INT REFERENCES educacion (Dimension_Educacion),
            Dimension_Demografia_Poblacion INT REFERENCES demografia_poblacion (Dimension_Demografia_Poblacion),
            Dimension_Vivienda_Servicios INT REFERENCES vivienda_servicios (Dimension_Vivienda_Servicios),
            Dimension_Salud INT REFERENCES salud (Dimension_Salud)
        );

    """
