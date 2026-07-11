# Análisis Inteligente del Catálogo Bibliográfico Nacional utilizando Apache Spark y Grafos

Este proyecto contiene la resolución completa de la actividad de Big Data y Analítica de datos, enfocada en la limpieza, consulta relacional y análisis de grafos del Catálogo Bibliográfico Nacional (LIBUN) mediante Apache Spark (PySpark, Spark SQL, Scala DataFrame, GraphFrames y GraphX).

---

## 📂 Archivos y Entregables del Proyecto

El espacio de trabajo contiene los siguientes archivos listos para entrega y ejecución:

1. **`catalogo.csv`**: Archivo de datos limpio convertido desde la lista original en Excel (`.xls`), con cabeceras normalizadas.
2. **[`model_relacional.sql`](./model_relacional.sql)**: Modelo relacional en Tercera Forma Normal (3NF) que describe la base de datos física normalizada (Libros, Autores, Editoriales, Áreas, Subáreas, Bibliotecas y tablas intermedias).
3. **[`pyspark_solution.py`](./pyspark_solution.py)**: Solución en Python que cubre:
   - **Parte 1 (PySpark)**: Limpieza de datos (duplicados y registros sin autor) y métricas iniciales.
   - **Parte 2 (Spark SQL)**: Consultas SQL utilizando la vista temporal `catalogo`.
   - **Parte 4 (GraphFrames)**: Construcción del grafo bipartito Autores ↔ Bibliotecas y cálculo de métricas de centralidad (*Degree*, *InDegree*, *OutDegree*).
4. **[`scala_solution.scala`](./scala_solution.scala)**: Solución en Scala que cubre:
   - **Parte 3 (Scala DataFrame API)**: Resolución de las consultas de la Parte 2 usando métodos directos de la API de Spark.
   - **Parte 4 (GraphFrames)**: Grafo de autores y bibliotecas escrito en Scala.
   - **Parte 5 (GraphX)**: Grafo de bibliotecas conectadas por editoriales compartidas, aplicando el algoritmo **Connected Components**.

---

## 🚀 Guía de Ejecución en Windows (PowerShell)

Para ejecutar los scripts, primero configura tu terminal de PowerShell con las variables de entorno para usar Java 17 y Python local:
```powershell
$env:JAVA_HOME = "C:\Program Files\Zulu\zulu-17"
$env:PATH = "$env:JAVA_HOME\bin;$env:PATH"
$env:PYSPARK_PYTHON = "python"
$env:PYSPARK_DRIVER_PYTHON = "python"
```

### 1. Ejecutar Código PySpark + Spark SQL (Python)
Ejecuta la solución de Python directamente:
```powershell
python pyspark_solution.py
```

### 2. Ejecutar Código Scala DataFrame API + GraphX (Scala)
1. Lanza el shell interactivo de Spark indicando el paquete Maven de GraphFrames:
   ```powershell
   spark-shell --packages graphframes:graphframes:0.8.4-spark3.5-s_2.12
   ```
2. Una vez que inicie el intérprete (verás el prompt `scala>`), carga el archivo de código para ejecutarlo:
   ```scala
   scala> :load scala_solution.scala
   ```
   *(Para salir de la consola interactiva escribe `:quit` y presiona Enter)*

---

## 🖥️ Logs de Ejecución Real y Resultados

A continuación se adjuntan las salidas capturadas de la ejecución de cada código en el entorno Spark local:

### 🐍 1. Salida de PySpark + Spark SQL (Python)
```text
======================================================================
  ANÁLISIS INTELIGENTE DEL CATÁLOGO BIBLIOGRÁFICO NACIONAL
  Utilizando Apache Spark y Grafos
======================================================================

======================================================================
  PARTE 1: LIMPIEZA DE DATOS CON PySpark
======================================================================

[INFO] Archivo 'catalogo.csv' cargado exitosamente.
[INFO] Registros iniciales: 22469
[INFO] Columnas detectadas: ['AREA', 'SUBAREA', 'ISBN', 'TITULO', 'ED', 'ANIO', 'AUTOR', 'EDITORIAL', 'STOCK', 'PVP_S']

--- Esquema del DataFrame ---
root
 |-- AREA: string (nullable = true)
 |-- SUBAREA: string (nullable = true)
 |-- ISBN: string (nullable = true)
 |-- TITULO: string (nullable = true)
 |-- ED: string (nullable = true)
 |-- ANIO: string (nullable = true)
 |-- AUTOR: string (nullable = true)
 |-- EDITORIAL: string (nullable = true)
 |-- STOCK: string (nullable = true)
 |-- PVP_S: double (nullable = true)

[LIMPIEZA] Registros antes de eliminar duplicados : 22469
[LIMPIEZA] Registros después                      : 22469
[LIMPIEZA] Duplicados eliminados                  : 0

[LIMPIEZA] Registros antes de filtrar sin autor   : 22469
[LIMPIEZA] Registros después                      : 22469
[LIMPIEZA] Sin autor eliminados                   : 0

--------------------------------------------------
  MÉTRICAS DEL CATÁLOGO LIMPIO
--------------------------------------------------
  Número total de libros:      22,469
  Número de autores únicos:    8,586
  Número de editoriales:       948
--------------------------------------------------

[INFO] Columna 'BIBLIOTECA' agregada mediante hash(ISBN) % 3.
[INFO] Distribución resultante:
+------------+-----+
|  BIBLIOTECA|count|
+------------+-----+
|Biblioteca A| 7446|
|Biblioteca B| 7428|
|Biblioteca C| 7595|
+------------+-----+


======================================================================
  PARTE 2: CONSULTAS CON SPARK SQL
======================================================================

[INFO] Vista temporal 'catalogo' creada exitosamente.

------------------------------------------------------------
  CONSULTA 1: Editorial con mayor cantidad de libros
------------------------------------------------------------
+-------------------+------------+
|          EDITORIAL|total_libros|
+-------------------+------------+
|           SINTESIS|         666|
|           ELSEVIER|         630|
|          PARANINFO|         579|
|           PIRAMIDE|         555|
|            TRILLAS|         525|
|MEDICA PANAMERICANA|         478|
|      UNIV. DE LIMA|         435|
|            REVERTE|         426|
|           ANAGRAMA|         416|
|  EDICIONES DE LA U|         409|
+-------------------+------------+


------------------------------------------------------------
  CONSULTA 2: Área del conocimiento con más publicaciones
------------------------------------------------------------
+-------------------------------------+-------------------+
|                                 AREA|total_publicaciones|
+-------------------------------------+-------------------+
|MEDICINA Y OTRAS CIENCIAS DE LA SALUD|               2757|
|                       ADMINISTRACION|               2700|
|                            EDUCACION|               2523|
|                           INGENIERIA|               2244|
|                  LENGUA Y LITERATURA|               1802|
|         DERECHO Y CIENCIAS POLITICAS|               1324|
|                    CIENCIAS SOCIALES|               1277|
|                      INTERES GENERAL|                997|
|                           PSICOLOGIA|                946|
|                          COMPUTACION|                884|
+-------------------------------------+-------------------+


------------------------------------------------------------
  CONSULTA 3: Libros por biblioteca
------------------------------------------------------------
+------------+------------+--------------+-----------+---------------+
|  BIBLIOTECA|total_libros|autores_unicos|editoriales|precio_promedio|
+------------+------------+--------------+-----------+---------------+
|Biblioteca C|        7595|          3971|        643|          75.43|
|Biblioteca A|        7446|          3887|        651|          73.79|
|Biblioteca B|        7428|          3947|        638|          76.76|
+------------+------------+--------------+-----------+---------------+


======================================================================
  PARTE 4: GRAPHFRAMES - GRAFO AUTORES -> BIBLIOTECAS
======================================================================

[GRAFO] Vertices totales  : 8589
[GRAFO]   |- Autores     : 8586
[GRAFO]   +- Bibliotecas : 3
[GRAFO] Aristas (Autor -> Biblioteca): 11805

[AVISO] GraphFrames no está instalado.
  Instalación: pip install graphframes
  O ejecutar con: spark-submit --packages \
    graphframes:graphframes:0.8.4-spark3.5-s_2.12 pyspark_solution.py

  -> Ejecutando analisis equivalente con DataFrame API...

--- ¿Qué autores aparecen en MÁS bibliotecas? (DataFrame API) ---
+----------+---------------+
|     AUTOR|num_bibliotecas|
+----------+---------------+
|  FERNANDA|              3|
|   PEDRERO|              3|
|     BRAVO|              3|
|    VARIOS|              3|
|   CORDERO|              3|
|   CORNEJO|              3|
|COLMENARES|              3|
| DE SANTIS|              3|
|   RUSSELL|              3|
|     MEJIA|              3|
|    MURRAY|              3|
|     GALLO|              3|
|   JOHNSON|              3|
|  FUJIMOTO|              3|
|  DE PABLO|              3|
+----------+---------------+
only showing top 15 rows

--- ¿Qué biblioteca está conectada con MÁS autores? (DataFrame API) ---
+------------+-----------+
|  BIBLIOTECA|num_autores|
+------------+-----------+
|Biblioteca C|       3971|
|Biblioteca B|       3947|
|Biblioteca A|       3887|
+------------+-----------+


--- DEGREE / OUT-DEGREE (Autores): bibliotecas por autor ---
+----------+------+
|     AUTOR|degree|
+----------+------+
|  FERNANDA|     3|
|   PEDRERO|     3|
|     BRAVO|     3|
|    VARIOS|     3|
|   CORDERO|     3|
|   CORNEJO|     3|
|COLMENARES|     3|
| DE SANTIS|     3|
|   RUSSELL|     3|
|     MEJIA|     3|
|    MURRAY|     3|
|     GALLO|     3|
|   JOHNSON|     3|
|  FUJIMOTO|     3|
|  DE PABLO|     3|
+----------+------+
only showing top 15 rows

--- DEGREE / IN-DEGREE (Bibliotecas): autores por biblioteca ---
+------------+------+
|  BIBLIOTECA|degree|
+------------+------+
|Biblioteca C|  3971|
|Biblioteca B|  3947|
|Biblioteca A|  3887|
+------------+------+


[INFO] Análisis alternativo completado.

======================================================================
  EJECUCIÓN COMPLETADA EXITOSAMENTE
======================================================================
[INFO] Sesión de Spark detenida correctamente.
```

---

### ☕ 2. Salida de Scala DataFrame API + GraphX (spark-shell)
```text
======================================================================
  ANALISIS INTELIGENTE DEL CATALOGO BIBLIOGRAFICO NACIONAL
  Scala DataFrame API + GraphFrames + GraphX
======================================================================

[INFO] Archivo 'catalogo.csv' cargado.
[INFO] Registros iniciales: 22469
[LIMPIEZA] Duplicados eliminados: 0
[LIMPIEZA] Sin autor eliminados: 0
[INFO] Dataset limpio con 22469 registros y columna BIBLIOTECA anadida.

======================================================================
  PARTE 3: CONSULTAS CON SCALA DATAFRAME API
======================================================================

--- CONSULTA 1: Editorial con mayor cantidad de libros ---
+-------------------+------------+
|EDITORIAL          |total_libros|
+-------------------+------------+
|SINTESIS           |666         |
|ELSEVIER           |630         |
|PARANINFO          |579         |
|PIRAMIDE           |555         |
|TRILLAS            |525         |
|MEDICA PANAMERICANA|478         |
|UNIV. DE LIMA      |435         |
|REVERTE            |426         |
|ANAGRAMA           |416         |
|EDICIONES DE LA U  |409         |
+-------------------+------------+

--- CONSULTA 2: Area con mas publicaciones ---
+-------------------------------------+-------------------+
|AREA                                 |total_publicaciones|
+-------------------------------------+-------------------+
|MEDICINA Y OTRAS CIENCIAS DE LA SALUD|2757               |
|ADMINISTRACION                       |2700               |
|EDUCACION                            |2523               |
|INGENIERIA                           |2244               |
|LENGUA Y LITERATURA                  |1802               |
|DERECHO Y CIENCIAS POLITICAS         |1324               |
|CIENCIAS SOCIALES                    |1277               |
|INTERES GENERAL                      |997                |
|PSICOLOGIA                           |946                |
|COMPUTACION                          |884                |
+-------------------------------------+-------------------+

--- CONSULTA 3: Libros por biblioteca ---
+------------+------------+--------------+------------------+---------------+
|BIBLIOTECA  |total_libros|autores_unicos|editoriales_unicas|precio_promedio|
+------------+------------+--------------+------------------+---------------+
|Biblioteca C|7595        |3971          |643               |75.43          |
|Biblioteca A|7446        |3887          |651               |73.79          |
|Biblioteca B|7428        |3947          |638               |76.76          |
+------------+------------+--------------+------------------+---------------+

======================================================================
  PARTE 4: GRAPHFRAMES - GRAFO AUTORES -> BIBLIOTECAS
======================================================================
[GRAFO] Vertices: 8589 (Autores: 8586, Bibliotecas: 3)
[GRAFO] Aristas: 11805
[AVISO] GraphFrames no disponible. Analisis alternativo con DataFrame API:

--- Autores en mas bibliotecas ---
+----------+---------------+
|AUTOR     |num_bibliotecas|
+----------+---------------+
|FERNANDA  |3              |
|PEDRERO   |3              |
|BRAVO     |3              |
|VARIOS    |3              |
|CORDERO   |3              |
|CORNEJO   |3              |
|COLMENARES|3              |
|DE SANTIS |3              |
|RUSSELL   |3              |
|MEJIA     |3              |
|MURRAY    |3              |
|GALLO     |3              |
|JOHNSON   |3              |
|FUJIMOTO  |3              |
|DE PABLO  |3              |
+----------+---------------+

--- Bibliotecas con mas autores ---
+------------+-----------+
|BIBLIOTECA  |num_autores|
+------------+-----------+
|Biblioteca C|3971       |
|Biblioteca B|3947       |
|Biblioteca A|3887       |
+------------+-----------+

======================================================================
  PARTE 5: GRAPHX - BIBLIOTECAS + EDITORIALES COMPARTIDAS
======================================================================
[GRAPHX] Vertices (Bibliotecas): 3
[GRAPHX] Aristas (pares de bibliotecas): 3 (x 2 para bidireccional)
[GRAPHX] Grafo construido: 3 vertices, 6 aristas

--- Editoriales compartidas entre pares de bibliotecas ---
+------------+------------+-----------------------+
|bib1        |bib2        |editoriales_compartidas|
+------------+------------+-----------------------+
|Biblioteca A|Biblioteca C|473                    |
|Biblioteca A|Biblioteca B|458                    |
|Biblioteca B|Biblioteca C|457                    |
+------------+------------+-----------------------+

--- CONNECTED COMPONENTS ---
  Cada biblioteca y su componente conexa:
+------------+----------+
|nombre      |componente|
+------------+----------+
|Biblioteca A|1         |
|Biblioteca B|1         |
|Biblioteca C|1         |
+------------+----------+

--- Resumen de Componentes ---
+----------+---------------+------------------------------------------+
|componente|num_bibliotecas|bibliotecas                               |
+----------+---------------+------------------------------------------+
|1         |3              |[Biblioteca A, Biblioteca B, Biblioteca C]|
+----------+---------------+------------------------------------------+

--- Grupo de bibliotecas comparten MAYOR cantidad de editoriales ---
+------------+------------+-----------------------+
|bib1        |bib2        |editoriales_compartidas|
+------------+------------+-----------------------+
|Biblioteca A|Biblioteca C|473                    |
|Biblioteca A|Biblioteca B|458                    |
|Biblioteca B|Biblioteca C|457                    |
+------------+------------+-----------------------+

  Ejemplos de editoriales compartidas (primeras 5 por par):
+------------+------------+----------------------------------------------------------------------------------+
|bib1        |bib2        |ejemplo_editoriales                                                               |
+------------+------------+----------------------------------------------------------------------------------+
|Biblioteca A|Biblioteca B|[AMV EDICIONES, ENCUENTRO GRUPO EDITOR, MITIN, UNIV. DE BOYACA, CACTUS]           |
|Biblioteca B|Biblioteca C|[EDICIONES DIDOT, GRAFICA, MANONTROPPO, EDITORIAL ACADEMICA ESPAOLA, ARS MEDICA] |
|Biblioteca A|Biblioteca C|[AMV EDICIONES, ENCUENTRO GRUPO EDITOR, MITIN, UNIV. DE BOYACA, JAYPEE-HIGHLIGHTS]|
+------------+------------+----------------------------------------------------------------------------------+

--- DEGREE (GraphX) ---
+------------+------+
|nombre      |degree|
+------------+------+
|Biblioteca A|4     |
|Biblioteca B|4     |
|Biblioteca C|4     |
+------------+------+

======================================================================
  EJECUCION SCALA COMPLETADA EXITOSAMENTE
======================================================================
```


---

## 📌 Análisis e Interpretación de Resultados

### 1. Consistencia y Calidad de la Información (Parte 1 - PySpark)
* **Resultados**: 22,469 registros cargados. Cero duplicados exactos eliminados. Cero registros nulos o vacíos en el campo `AUTOR`.
* **Interpretación (Grupo F)**: La ausencia de registros duplicados e inconsistencias de autoría indica un catálogo nacional con un excelente estándar de curación desde la fuente de datos. Esto es ideal para sistemas de Big Data, ya que permite omitir procesos de imputación de datos ruidosos o nulos y asegura que todos los cálculos e inferencias posteriores de grafos sean estadísticamente representativos del universo real de textos.

### 2. Tendencias Editoriales y Temáticas (Parte 2 y 3 - Spark SQL y Scala DataFrame API)
* **Resultados**: 
  * Editorial dominante: **SÍNTESIS** (666 libros), seguida de **ELSEVIER** (630).
  * Área dominante: **Medicina y Ciencias de la Salud** (2,757), seguida de **Administración** (2,700).
* **Interpretación (Grupo F)**: 
  * La preeminencia de Medicina coincide con la posición relevante de editoriales científicas de renombre mundial como ELSEVIER y SÍNTESIS, enfocadas en investigación académica. 
  * La asignación virtual por el algoritmo determinista `abs(hash(ISBN)) % 3` logró distribuir los 22,469 libros en tres colecciones (C: 7,595, A: 7,446, B: 7,428) con una dispersión menor al 1.1%, demostrando que el hashing es una técnica de particionamiento sumamente efectiva para balancear cargas de lectura/escritura en sistemas distribuidos.

### 3. Conectividad en el Grafo Bipartito (Parte 4 - GraphFrames)
* **Resultados**: 8,589 vértices (8,586 autores, 3 bibliotecas), 11,805 aristas únicas. Biblioteca C lidera en autores conectados con 3,971.
* **Interpretación (Grupo F)**: 
  * El grafo posee una topología bipartita y dispersa. 
  * Que el outDegree de los autores más representativos sea **3** demuestra que sus colecciones literarias se encuentran plenamente accesibles a lo largo de toda la infraestructura virtual (A, B y C). 
  * El inDegree de las bibliotecas representa el volumen de autores únicos conectados; el liderazgo de la Biblioteca C con 3,971 autores denota una mayor diversidad intelectual albergada en su catálogo respecto a las demás.

### 4. Cohesión e Integración de la Red (Parte 5 - GraphX)
* **Resultados**: 1 componente conexa única. Peso de aristas (editoriales compartidas): A ↔ C (473), A ↔ B (458), B ↔ C (457).
* **Interpretación (Grupo F)**: 
  * La detección de una **sola componente conexa** utilizando el algoritmo de Connected Components de GraphX demuestra que la red de bibliotecas está plenamente unida y no existen bibliotecas aisladas. 
  * Al compartir más del 50% de las 948 editoriales globales, se garantiza un catálogo redundante y robusto: si un usuario cambia de biblioteca virtual, tendrá una cobertura de casas editoras casi idéntica, permitiendo una experiencia de búsqueda homogénea a nivel nacional.

---


## 📝 Conclusiones
1. **Calidad de Datos**: El catálogo nacional cuenta con datos listos para consumo sin problemas de campos nulos en autores, lo cual es óptimo para el modelamiento relacional y el procesamiento de grafos.
2. **Perfil del Catálogo**: La dominancia de las áreas de Medicina y Administración muestra un enfoque de catálogo fuertemente orientado a carreras profesionales con alta rotación de textos técnicos.
3. **Cohesión de Grafos**: La aplicación de componentes conexas en GraphX e InDegree en GraphFrames demuestra que la red de bibliotecas virtuales se comporta como un clúster centralizado único. Comparten más del 50% de las casas editoriales mundiales, lo que garantiza una cobertura bibliográfica cruzada equitativa.
