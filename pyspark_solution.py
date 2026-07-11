# ============================================================
# ANÁLISIS INTELIGENTE DEL CATÁLOGO BIBLIOGRÁFICO NACIONAL
# Utilizando Apache Spark y Grafos
# ============================================================
# Curso:     Big Data y Analítica de Datos
# Actividad: Semana 14 – Apache Spark
# ============================================================
#
# Este script implementa:
#   ▸ Parte 1 (PySpark)     – Limpieza de datos
#   ▸ Parte 2 (Spark SQL)   – Consultas sobre vista temporal "catalogo"
#   ▸ Parte 4 (GraphFrames) – Grafo bipartito Autores → Bibliotecas
#
# Requisitos:
#   pip install pyspark
#   pip install graphframes   (opcional; si no está disponible,
#                               se ejecuta el análisis equivalente
#                               con la API DataFrame)
#
# Ejecución:
#   python pyspark_solution.py
#   O bien:
#   spark-submit --packages graphframes:graphframes:0.8.4-spark3.5-s_2.12 \
#                pyspark_solution.py
# ============================================================

# ─── Importaciones ──────────────────────────────────────────
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, abs as spark_abs, hash as spark_hash,
    when, lit, count, countDistinct, desc
)

import os

# Configurar HADOOP_HOME programáticamente para evitar el error de winutils en Windows
workspace_dir = r"d:\UPAO\ISIA\Octavo\Big Data y Analítica de datos\Semana 14\Apache Spark"
os.environ["HADOOP_HOME"] = os.path.join(workspace_dir, "hadoop")
os.environ["PATH"] = os.path.join(os.environ["HADOOP_HOME"], "bin") + ";" + os.environ.get("PATH", "")

# ============================================================
# INICIALIZACIÓN DE LA SESIÓN SPARK
# ============================================================
# .master("local[*]") → utiliza todos los cores de la máquina
# .appName(...)       → nombre descriptivo visible en la Spark UI
try:
    spark = (
        SparkSession.builder
        .appName("CatalogoBibliograficoNacional")
        .master("local[*]")
        .config("spark.jars.packages", "graphframes:graphframes:0.8.4-spark3.5-s_2.12")
        .getOrCreate()
    )
    print("[INFO] Sesión de Spark inicializada con soporte nativo de GraphFrames.")
except Exception as e:
    print(f"[ADVERTENCIA] No se pudo inicializar con soporte de GraphFrames: {e}")
    print("[ADVERTENCIA] Inicializando sesión de Spark estándar.")
    spark = (
        SparkSession.builder
        .appName("CatalogoBibliograficoNacional")
        .master("local[*]")
        .getOrCreate()
    )

# Reducir la verbosidad de logs (solo warnings y errores)
spark.sparkContext.setLogLevel("WARN")

print("=" * 70)
print("  ANÁLISIS INTELIGENTE DEL CATÁLOGO BIBLIOGRÁFICO NACIONAL")
print("  Utilizando Apache Spark y Grafos")
print("=" * 70)


# ╔══════════════════════════════════════════════════════════════╗
# ║  PARTE 1: PySpark — LIMPIEZA DE DATOS                       ║
# ╚══════════════════════════════════════════════════════════════╝
print("\n" + "=" * 70)
print("  PARTE 1: LIMPIEZA DE DATOS CON PySpark")
print("=" * 70)

# ── 1.1  Lectura del archivo CSV ─────────────────────────────
# El archivo fue convertido desde el Excel original
# (.xls → .csv) con las siguientes columnas:
#   AREA, SUBAREA, ISBN, TITULO, ED, ANIO,
#   AUTOR, EDITORIAL, STOCK, PVP_S
#
# Opciones de lectura:
#   header=true      → la primera fila contiene encabezados
#   inferSchema=true → Spark deduce el tipo de cada columna
#   encoding=UTF-8   → asegurar lectura correcta de caracteres
csv_path = "catalogo.csv"

df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("encoding", "UTF-8")
    .csv(csv_path)
)

print(f"\n[INFO] Archivo '{csv_path}' cargado exitosamente.")
print(f"[INFO] Registros iniciales: {df.count()}")
print(f"[INFO] Columnas detectadas: {df.columns}")

# Mostrar el esquema inferido por Spark
print("\n--- Esquema del DataFrame ---")
df.printSchema()

# Mostrar las primeras 5 filas como muestra
print("--- Primeras 5 filas ---")
df.show(5, truncate=40)

# ── 1.2  Eliminación de registros duplicados ─────────────────
# dropDuplicates() compara TODAS las columnas de cada fila;
# si dos filas son idénticas en cada campo, solo conserva una.
registros_antes = df.count()
df = df.dropDuplicates()
registros_despues = df.count()
duplicados_eliminados = registros_antes - registros_despues

print(f"\n[LIMPIEZA] Registros antes de eliminar duplicados : {registros_antes}")
print(f"[LIMPIEZA] Registros después                      : {registros_despues}")
print(f"[LIMPIEZA] Duplicados eliminados                  : {duplicados_eliminados}")

# ── 1.3  Eliminación de registros sin autor ──────────────────
# Se filtran filas donde AUTOR sea NULL o una cadena vacía/espacios.
# trim() elimina espacios en blanco al inicio y al final.
registros_antes = df.count()
df = df.filter(
    col("AUTOR").isNotNull() &        # Descartar NULL
    (trim(col("AUTOR")) != "")        # Descartar cadenas vacías
)
registros_despues = df.count()
sin_autor = registros_antes - registros_despues

print(f"\n[LIMPIEZA] Registros antes de filtrar sin autor   : {registros_antes}")
print(f"[LIMPIEZA] Registros después                      : {registros_despues}")
print(f"[LIMPIEZA] Sin autor eliminados                   : {sin_autor}")

# ── 1.4  Métricas del catálogo limpio ────────────────────────
# Se calcula:
#   - Número total de libros       → count() sobre el DataFrame
#   - Número de autores únicos     → distinct() sobre columna AUTOR
#   - Número de editoriales únicas → distinct() sobre columna EDITORIAL
num_libros      = df.count()
num_autores     = df.select("AUTOR").distinct().count()
num_editoriales = df.select("EDITORIAL").distinct().count()

print("\n" + "-" * 50)
print("  MÉTRICAS DEL CATÁLOGO LIMPIO")
print("-" * 50)
print(f"  Número total de libros:      {num_libros:,}")
print(f"  Número de autores únicos:    {num_autores:,}")
print(f"  Número de editoriales:       {num_editoriales:,}")
print("-" * 50)

# ── 1.5  Asignación de bibliotecas virtuales ─────────────────
# NOTA DE DISEÑO:
#   El dataset original NO contiene una columna "Biblioteca".
#   Para responder las preguntas de las Partes 2–5 que involucran
#   bibliotecas, se asigna cada registro a una de 3 bibliotecas
#   virtuales de forma DETERMINISTA usando la función hash() de
#   Spark sobre el ISBN.
#
#   Fórmula:
#     abs(hash(ISBN)) % 3  →  0 = "Biblioteca A"
#                              1 = "Biblioteca B"
#                              2 = "Biblioteca C"
#
#   Ventajas de este enfoque:
#     • Determinista: el mismo ISBN siempre cae en la misma biblioteca.
#     • Uniforme: distribución aproximadamente equitativa.
#     • Reproducible: cualquier entorno Spark dará el mismo resultado.
df = df.withColumn(
    "BIBLIOTECA",
    when(spark_abs(spark_hash(col("ISBN"))) % 3 == 0, lit("Biblioteca A"))
    .when(spark_abs(spark_hash(col("ISBN"))) % 3 == 1, lit("Biblioteca B"))
    .otherwise(lit("Biblioteca C"))
)

print("\n[INFO] Columna 'BIBLIOTECA' agregada mediante hash(ISBN) % 3.")
print("[INFO] Distribución resultante:")
df.groupBy("BIBLIOTECA").count().orderBy("BIBLIOTECA").show()


# ╔══════════════════════════════════════════════════════════════╗
# ║  PARTE 2: SPARK SQL — CONSULTAS                             ║
# ╚══════════════════════════════════════════════════════════════╝
print("\n" + "=" * 70)
print("  PARTE 2: CONSULTAS CON SPARK SQL")
print("=" * 70)

# ── 2.0  Crear la vista temporal "catalogo" ──────────────────
# createOrReplaceTempView() registra el DataFrame como una tabla
# SQL virtual en el catálogo de Spark, accesible vía spark.sql().
# La vista persiste solo durante la sesión activa.
df.createOrReplaceTempView("catalogo")
print("\n[INFO] Vista temporal 'catalogo' creada exitosamente.")

# ── 2.1  ¿Qué editorial posee mayor cantidad de libros? ─────
# Se agrupa por EDITORIAL, se cuenta la cantidad de registros,
# y se ordena de mayor a menor para identificar la editorial líder.
print("\n" + "-" * 60)
print("  CONSULTA 1: Editorial con mayor cantidad de libros")
print("-" * 60)

query_editorial = """
    SELECT
        EDITORIAL,                       -- Nombre de la editorial
        COUNT(*) AS total_libros         -- Cantidad de libros publicados
    FROM catalogo
    GROUP BY EDITORIAL                   -- Agrupamiento por editorial
    ORDER BY total_libros DESC           -- Orden descendente (la mayor primero)
    LIMIT 10                             -- Top 10 editoriales
"""
spark.sql(query_editorial).show(10, truncate=50)

# ── 2.2  ¿Qué área del conocimiento tiene más publicaciones? ─
# Misma lógica: agrupamos por AREA y contamos publicaciones.
print("\n" + "-" * 60)
print("  CONSULTA 2: Área del conocimiento con más publicaciones")
print("-" * 60)

query_area = """
    SELECT
        AREA,                            -- Área del conocimiento
        COUNT(*) AS total_publicaciones  -- Cantidad de publicaciones
    FROM catalogo
    GROUP BY AREA
    ORDER BY total_publicaciones DESC
    LIMIT 10
"""
spark.sql(query_area).show(10, truncate=50)

# ── 2.3  ¿Cuántos libros existen por biblioteca? ─────────────
# Además de contar libros, añadimos métricas complementarias:
# autores únicos, editoriales únicas y precio promedio por biblioteca.
print("\n" + "-" * 60)
print("  CONSULTA 3: Libros por biblioteca")
print("-" * 60)

query_biblioteca = """
    SELECT
        BIBLIOTECA,                                -- Nombre de la biblioteca
        COUNT(*)              AS total_libros,      -- Total de libros
        COUNT(DISTINCT AUTOR) AS autores_unicos,    -- Autores distintos
        COUNT(DISTINCT EDITORIAL) AS editoriales,   -- Editoriales distintas
        ROUND(AVG(PVP_S), 2) AS precio_promedio     -- Precio medio de venta
    FROM catalogo
    GROUP BY BIBLIOTECA
    ORDER BY total_libros DESC
"""
spark.sql(query_biblioteca).show(truncate=50)


# ╔══════════════════════════════════════════════════════════════╗
# ║  PARTE 4: GRAPHFRAMES — GRAFO AUTORES → BIBLIOTECAS         ║
# ╚══════════════════════════════════════════════════════════════╝
print("\n" + "=" * 70)
print("  PARTE 4: GRAPHFRAMES - GRAFO AUTORES -> BIBLIOTECAS")
print("=" * 70)

# ── Nota sobre GraphFrames ───────────────────────────────────
# GraphFrames es una librería de grafos para Apache Spark que
# trabaja sobre DataFrames (a diferencia de GraphX que usa RDDs).
#
# Estructura del grafo bipartito:
#   Vértices = { Autores } ∪ { Bibliotecas }
#   Aristas  = { (Autor, Biblioteca) : el autor tiene libros en esa biblioteca }
#
# Si GraphFrames no está instalado, se ejecuta un análisis
# equivalente usando operaciones de la API DataFrame.

try:
    from graphframes import GraphFrame

    # ── 4.1  Construcción de vértices ────────────────────────
    # GraphFrame exige que los vértices tengan una columna "id".
    # Creamos dos conjuntos de vértices con una columna "tipo"
    # para distinguir autores de bibliotecas.

    # Vértices de autores (cada autor único es un nodo)
    v_autores = (
        df.select(col("AUTOR").alias("id"))
        .distinct()
        .withColumn("tipo", lit("autor"))
    )

    # Vértices de bibliotecas (3 nodos: Biblioteca A, B, C)
    v_bibliotecas = (
        df.select(col("BIBLIOTECA").alias("id"))
        .distinct()
        .withColumn("tipo", lit("biblioteca"))
    )

    # Unión de todos los vértices en un solo DataFrame
    vertices = v_autores.union(v_bibliotecas)

    print(f"\n[GRAFO] Vertices totales  : {vertices.count()}")
    print(f"[GRAFO]   |- Autores     : {v_autores.count()}")
    print(f"[GRAFO]   +- Bibliotecas : {v_bibliotecas.count()}")

    # ── 4.2  Construcción de aristas (edges) ─────────────────
    # Cada arista va de un Autor (src) a una Biblioteca (dst).
    # Se usa DISTINCT para eliminar aristas duplicadas
    # (un autor puede tener múltiples libros en la misma biblioteca,
    #  pero solo queremos una arista por par único).
    aristas = (
        df.select(
            col("AUTOR").alias("src"),        # Nodo origen → Autor
            col("BIBLIOTECA").alias("dst")    # Nodo destino → Biblioteca
        )
        .distinct()
    )

    print(f"[GRAFO] Aristas (Autor -> Biblioteca): {aristas.count()}")

    # ── 4.3  Crear el GraphFrame ─────────────────────────────
    grafo = GraphFrame(vertices, aristas)

    # ── 4.4  ¿Qué autores aparecen en más bibliotecas? ──────
    # Agrupamos las aristas por "src" (autor) y contamos
    # cuántas bibliotecas distintas ("dst") tiene cada uno.
    print("\n--- ¿Qué autores aparecen en MÁS bibliotecas? ---")
    autores_en_bib = (
        grafo.edges
        .groupBy("src")
        .agg(count("dst").alias("num_bibliotecas"))
        .orderBy(desc("num_bibliotecas"))
    )
    autores_en_bib.show(15, truncate=50)

    # ── 4.5  ¿Qué biblioteca tiene más autores conectados? ──
    # Agrupamos por "dst" (biblioteca) y contamos los autores.
    print("\n--- ¿Qué biblioteca está conectada con MÁS autores? ---")
    bib_con_autores = (
        grafo.edges
        .groupBy("dst")
        .agg(count("src").alias("num_autores"))
        .orderBy(desc("num_autores"))
    )
    bib_con_autores.show(truncate=50)

    # ── 4.6  Métricas de Centralidad ────────────────────────

    # DEGREE (Grado total)
    # El grado de un nodo es la cantidad total de aristas que lo
    # conectan con otros nodos (entrantes + salientes).
    # Para un grafo bipartito:
    #   - Grado de un Autor    = cantidad de bibliotecas donde aparece
    #   - Grado de una Biblioteca = cantidad de autores que posee
    print("\n--- DEGREE: Grado total de cada nodo (top 15) ---")
    grafo.degrees.orderBy(desc("degree")).show(15, truncate=50)

    # IN-DEGREE (Grado de entrada)
    # Número de aristas que LLEGAN a un nodo.
    # En este grafo dirigido (Autor → Biblioteca):
    #   - Las BIBLIOTECAS reciben aristas → su inDegree indica
    #     cuántos autores distintos publican en ellas.
    #   - Los AUTORES no reciben aristas → inDegree = 0 o ausente.
    print("\n--- IN-DEGREE: Grado de entrada (aristas que llegan) ---")
    print("  Para BIBLIOTECAS: cuántos autores publican en cada una")
    grafo.inDegrees.orderBy(desc("inDegree")).show(10, truncate=50)

    # OUT-DEGREE (Grado de salida)
    # Número de aristas que SALEN de un nodo.
    # En este grafo:
    #   - Los AUTORES emiten aristas → su outDegree indica
    #     en cuántas bibliotecas aparecen.
    #   - Las BIBLIOTECAS no emiten aristas → outDegree = 0 o ausente.
    print("\n--- OUT-DEGREE: Grado de salida (aristas que salen) ---")
    print("  Para AUTORES: en cuántas bibliotecas aparecen")
    grafo.outDegrees.orderBy(desc("outDegree")).show(10, truncate=50)

    print("\n[GRAFO] Análisis GraphFrames completado exitosamente.")

except Exception:
    # ── Alternativa sin GraphFrames ──────────────────────────
    # Si GraphFrames no está instalado, realizamos el mismo
    # análisis usando operaciones nativas de DataFrame.
    print("\n[AVISO] GraphFrames no está instalado.")
    print("  Instalación: pip install graphframes")
    print("  O ejecutar con: spark-submit --packages \\")
    print("    graphframes:graphframes:0.8.4-spark3.5-s_2.12 pyspark_solution.py")
    print("\n  -> Ejecutando analisis equivalente con DataFrame API...\n")

    # Preparar la relación Autor → Biblioteca (sin duplicados)
    autor_bib = df.select("AUTOR", "BIBLIOTECA").distinct()

    # ¿Qué autores aparecen en más bibliotecas?
    print("--- ¿Qué autores aparecen en MÁS bibliotecas? (DataFrame API) ---")
    (autor_bib
     .groupBy("AUTOR")
     .agg(count("BIBLIOTECA").alias("num_bibliotecas"))
     .orderBy(desc("num_bibliotecas"))
     .show(15, truncate=50))

    # ¿Qué biblioteca está conectada con más autores?
    print("\n--- ¿Qué biblioteca está conectada con MÁS autores? (DataFrame API) ---")
    (autor_bib
     .groupBy("BIBLIOTECA")
     .agg(count("AUTOR").alias("num_autores"))
     .orderBy(desc("num_autores"))
     .show(truncate=50))

    # DEGREE simulado para autores (= OutDegree en el grafo dirigido)
    print("\n--- DEGREE / OUT-DEGREE (Autores): bibliotecas por autor ---")
    (autor_bib
     .groupBy("AUTOR")
     .agg(count("*").alias("degree"))
     .orderBy(desc("degree"))
     .show(15, truncate=50))

    # DEGREE simulado para bibliotecas (= InDegree en el grafo dirigido)
    print("\n--- DEGREE / IN-DEGREE (Bibliotecas): autores por biblioteca ---")
    (autor_bib
     .groupBy("BIBLIOTECA")
     .agg(count("*").alias("degree"))
     .orderBy(desc("degree"))
     .show(truncate=50))

    print("\n[INFO] Análisis alternativo completado.")


# ╔══════════════════════════════════════════════════════════════╗
# ║  FINALIZACIÓN                                                ║
# ╚══════════════════════════════════════════════════════════════╝
print("\n" + "=" * 70)
print("  EJECUCIÓN COMPLETADA EXITOSAMENTE")
print("=" * 70)

# Detener la sesión de Spark para liberar memoria y recursos
spark.stop()
print("[INFO] Sesión de Spark detenida correctamente.")
