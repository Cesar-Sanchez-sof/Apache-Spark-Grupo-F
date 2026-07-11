// ============================================================
// ANALISIS INTELIGENTE DEL CATALOGO BIBLIOGRAFICO NACIONAL
// Utilizando Apache Spark y Grafos
// ============================================================
// Curso:     Big Data y Analitica de Datos
// Actividad: Semana 14 - Apache Spark
// ============================================================
//
// Este script implementa:
//   * Parte 3 (Scala DataFrame API) - Consultas equivalentes a Spark SQL
//   * Parte 4 (GraphFrames)         - Grafo bipartito Autores -> Bibliotecas
//   * Parte 5 (GraphX)              - Grafo de Bibliotecas con Editoriales Compartidas
//                                      + Connected Components
//
// Ejecucion en spark-shell:
//   spark-shell --packages graphframes:graphframes:0.8.4-spark3.5-s_2.12
//   :load scala_solution.scala
// ============================================================

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// Importaciones para GraphX (Parte 5)
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

// Importar implicits para usar el signo $ en columnas
import spark.implicits._

// Reducir la verbosidad de los logs
spark.sparkContext.setLogLevel("WARN")

println("=" * 70)
println("  ANALISIS INTELIGENTE DEL CATALOGO BIBLIOGRAFICO NACIONAL")
println("  Scala DataFrame API + GraphFrames + GraphX")
println("=" * 70)


// ============================================================
// CARGA Y LIMPIEZA DE DATOS (preparacion comun)
// ============================================================

val csvPath = "catalogo.csv"

var df = spark.read
  .option("header", "true")         // La fila 1 contiene los nombres de columnas
  .option("inferSchema", "true")    // Spark infiere String, Int, Double, etc.
  .option("encoding", "UTF-8")      // Codificacion UTF-8
  .csv(csvPath)

println(s"\n[INFO] Archivo '${csvPath}' cargado.")
println(s"[INFO] Registros iniciales: ${df.count()}")

// -- Eliminacion de duplicados
val antesDedup = df.count()
df = df.dropDuplicates()
val despuesDedup = df.count()
println(s"[LIMPIEZA] Duplicados eliminados: ${antesDedup - despuesDedup}")

// -- Eliminacion de registros sin autor
val antesAutor = df.count()
df = df.filter(
  col("AUTOR").isNotNull &&            // Descartar NULL
  trim(col("AUTOR")) =!= ""            // Descartar vacios
)
val despuesAutor = df.count()
println(s"[LIMPIEZA] Sin autor eliminados: ${antesAutor - despuesAutor}")

// -- Asignacion de Bibliotecas virtuales
df = df.withColumn("BIBLIOTECA",
  when(abs(hash(col("ISBN"))) % 3 === 0, lit("Biblioteca A"))
  .when(abs(hash(col("ISBN"))) % 3 === 1, lit("Biblioteca B"))
  .otherwise(lit("Biblioteca C"))
)

println(s"[INFO] Dataset limpio con ${df.count()} registros y columna BIBLIOTECA anadida.\n")


// ============================================================
// PARTE 3: SCALA DATAFRAME API - CONSULTAS
// ============================================================
println("=" * 70)
println("  PARTE 3: CONSULTAS CON SCALA DATAFRAME API")
println("=" * 70)

// -- 3.1  Editorial con mayor cantidad de libros
println("\n--- CONSULTA 1: Editorial con mayor cantidad de libros ---")
val editorialTop = df
  .groupBy("EDITORIAL")
  .agg(count("*").alias("total_libros"))
  .orderBy(desc("total_libros"))
  .limit(10)

editorialTop.show(10, truncate = false)

// -- 3.2  Area del conocimiento con mas publicaciones
println("\n--- CONSULTA 2: Area con mas publicaciones ---")
val areaTop = df
  .groupBy("AREA")
  .agg(count("*").alias("total_publicaciones"))
  .orderBy(desc("total_publicaciones"))
  .limit(10)

areaTop.show(10, truncate = false)

// -- 3.3  Libros por biblioteca
println("\n--- CONSULTA 3: Libros por biblioteca ---")
val librosPorBib = df
  .groupBy("BIBLIOTECA")
  .agg(
    count("*").alias("total_libros"),
    countDistinct("AUTOR").alias("autores_unicos"),
    countDistinct("EDITORIAL").alias("editoriales_unicas"),
    round(avg("PVP_S"), 2).alias("precio_promedio")
  )
  .orderBy(desc("total_libros"))

librosPorBib.show(truncate = false)


// ============================================================
// PARTE 4: GRAPHFRAMES - GRAFO AUTORES -> BIBLIOTECAS
// ============================================================
println("\n" + "=" * 70)
println("  PARTE 4: GRAPHFRAMES - GRAFO AUTORES -> BIBLIOTECAS")
println("=" * 70)

try {
  // Importar GraphFrames
  import org.graphframes.GraphFrame

  // -- 4.1  Vertices
  val vAutores = df.select(col("AUTOR").alias("id"))
    .distinct()
    .withColumn("tipo", lit("autor"))

  val vBibliotecas = df.select(col("BIBLIOTECA").alias("id"))
    .distinct()
    .withColumn("tipo", lit("biblioteca"))

  val vertices = vAutores.union(vBibliotecas)
  println(s"\n[GRAFO] Vertices: ${vertices.count()} (Autores: ${vAutores.count()}, Bibliotecas: ${vBibliotecas.count()})")

  // -- 4.2  Aristas (Edges)
  val aristas = df.select(
    col("AUTOR").alias("src"),        // Origen: autor
    col("BIBLIOTECA").alias("dst")    // Destino: biblioteca
  ).distinct()

  println(s"[GRAFO] Aristas: ${aristas.count()}")

  // -- 4.3  Construir el GraphFrame
  val grafo = GraphFrame(vertices, aristas)

  // -- 4.4  Autores en mas bibliotecas
  println("\n--- Autores aparecen en MAS bibliotecas ---")
  grafo.edges
    .groupBy("src")
    .agg(count("dst").alias("num_bibliotecas"))
    .orderBy(desc("num_bibliotecas"))
    .show(15, truncate = false)

  // -- 4.5  Biblioteca con mas autores
  println("\n--- Biblioteca conectada con MAS autores ---")
  grafo.edges
    .groupBy("dst")
    .agg(count("src").alias("num_autores"))
    .orderBy(desc("num_autores"))
    .show(truncate = false)

  // -- 4.6  Degree, InDegree, OutDegree
  println("\n--- DEGREE (top 15) ---")
  grafo.degrees.orderBy(desc("degree")).show(15, truncate = false)

  println("\n--- IN-DEGREE (top 10) ---")
  grafo.inDegrees.orderBy(desc("inDegree")).show(10, truncate = false)

  println("\n--- OUT-DEGREE (top 10) ---")
  grafo.outDegrees.orderBy(desc("outDegree")).show(10, truncate = false)

  println("[GRAFO] Analisis GraphFrames (Parte 4) completado.")

} catch {
  case _: Throwable =>
    println("\n[AVISO] GraphFrames no disponible. Analisis alternativo con DataFrame API:")

    val autorBib = df.select("AUTOR", "BIBLIOTECA").distinct()

    println("\n--- Autores en mas bibliotecas ---")
    autorBib.groupBy("AUTOR")
      .agg(count("BIBLIOTECA").alias("num_bibliotecas"))
      .orderBy(desc("num_bibliotecas"))
      .show(15, truncate = false)

    println("\n--- Bibliotecas con mas autores ---")
    autorBib.groupBy("BIBLIOTECA")
      .agg(count("AUTOR").alias("num_autores"))
      .orderBy(desc("num_autores"))
      .show(truncate = false)
}


// ============================================================
// PARTE 5: GRAPHX - BIBLIOTECAS + EDITORIALES COMPARTIDAS
// ============================================================
println("\n" + "=" * 70)
println("  PARTE 5: GRAPHX - BIBLIOTECAS + EDITORIALES COMPARTIDAS")
println("=" * 70)

// -- 5.1  Mapeo de Bibliotecas a VertexId (Long)
val bibliotecaIds: Map[String, Long] = Map(
  "Biblioteca A" -> 1L,
  "Biblioteca B" -> 2L,
  "Biblioteca C" -> 3L
)

// -- 5.2  Crear el RDD de vertices
val verticesRDD: RDD[(VertexId, String)] = spark.sparkContext.parallelize(
  bibliotecaIds.map { case (nombre, id) => (id, nombre) }.toSeq
)
println(s"\n[GRAPHX] Vertices (Bibliotecas): ${verticesRDD.count()}")

// -- 5.3  Encontrar editoriales compartidas entre bibliotecas
val editBib = df.select("EDITORIAL", "BIBLIOTECA").distinct()

val paresCompartidos = editBib.alias("e1")
  .join(
    editBib.alias("e2"),
    col("e1.EDITORIAL") === col("e2.EDITORIAL") &&   // Misma editorial
    col("e1.BIBLIOTECA") < col("e2.BIBLIOTECA")       // Evitar duplicados
  )
  .select(
    col("e1.BIBLIOTECA").alias("bib1"),
    col("e2.BIBLIOTECA").alias("bib2"),
    col("e1.EDITORIAL").alias("editorial")
  )

println("\n--- Editoriales compartidas entre pares de bibliotecas ---")
val resumenPares = paresCompartidos
  .groupBy("bib1", "bib2")
  .agg(
    count("editorial").alias("editoriales_compartidas"),
    collect_list("editorial").alias("lista_editoriales")
  )
  .orderBy(desc("editoriales_compartidas"))

resumenPares.select("bib1", "bib2", "editoriales_compartidas").show(truncate = false)

// -- 5.4  Crear el RDD de aristas
val aristasData = paresCompartidos
  .groupBy("bib1", "bib2")
  .agg(count("editorial").alias("peso"))
  .collect()

val edgesList = aristasData.flatMap { row =>
  val b1 = row.getAs[String]("bib1")
  val b2 = row.getAs[String]("bib2")
  val peso = row.getAs[Long]("peso")
  val id1 = bibliotecaIds(b1)
  val id2 = bibliotecaIds(b2)
  Seq(
    Edge(id1, id2, peso),
    Edge(id2, id1, peso)
  )
}

val aristasRDD: RDD[Edge[Long]] = spark.sparkContext.parallelize(edgesList.toSeq)
println(s"[GRAPHX] Aristas (pares de bibliotecas): ${aristasRDD.count() / 2} (x 2 para bidireccional)")

// -- 5.5  Construir el grafo GraphX
val grafoX: Graph[String, Long] = Graph(verticesRDD, aristasRDD)

println(s"[GRAPHX] Grafo construido: ${grafoX.vertices.count()} vertices, ${grafoX.edges.count()} aristas")

// -- 5.6  Aplicar Connected Components
println("\n--- CONNECTED COMPONENTS ---")
val cc = grafoX.connectedComponents()

val componentesDF = cc.vertices.toDF("id", "componente")
  .join(
    verticesRDD.toDF("id_v", "nombre"),
    col("id") === col("id_v")
  )
  .select("nombre", "componente")
  .orderBy("componente", "nombre")

println("  Cada biblioteca y su componente conexa:")
componentesDF.show(truncate = false)

println("--- Resumen de Componentes ---")
componentesDF.groupBy("componente")
  .agg(
    count("nombre").alias("num_bibliotecas"),
    collect_list("nombre").alias("bibliotecas")
  )
  .orderBy(desc("num_bibliotecas"))
  .show(truncate = false)

// -- 5.7  Que grupos comparten mas editoriales?
println("\n--- Grupo de bibliotecas comparten MAYOR cantidad de editoriales ---")
resumenPares.select("bib1", "bib2", "editoriales_compartidas").show(truncate = false)

println("  Ejemplos de editoriales compartidas (primeras 5 por par):")
paresCompartidos
  .groupBy("bib1", "bib2")
  .agg(slice(collect_list("editorial"), 1, 5).alias("ejemplo_editoriales"))
  .show(truncate = false)


// ============================================================
// METRICAS ADICIONALES DE GRAPHX
// ============================================================

println("\n--- DEGREE (GraphX) ---")
val degreesX = grafoX.degrees
val degreeConNombres = degreesX.toDF("id", "degree")
  .join(verticesRDD.toDF("id_v", "nombre"), col("id") === col("id_v"))
  .select("nombre", "degree")
  .orderBy(desc("degree"))

degreeConNombres.show(truncate = false)

println("\n--- IN-DEGREE (GraphX) ---")
val inDegreesX = grafoX.inDegrees.toDF("id", "inDegree")
  .join(verticesRDD.toDF("id_v", "nombre"), col("id") === col("id_v"))
  .select("nombre", "inDegree")
  .orderBy(desc("inDegree"))
inDegreesX.show(truncate = false)

println("\n--- OUT-DEGREE (GraphX) ---")
val outDegreesX = grafoX.outDegrees.toDF("id", "outDegree")
  .join(verticesRDD.toDF("id_v", "nombre"), col("id") === col("id_v"))
  .select("nombre", "outDegree")
  .orderBy(desc("outDegree"))
outDegreesX.show(truncate = false)


// ============================================================
// FINALIZACION
// ============================================================
println("\n" + "=" * 70)
println("  EJECUCION SCALA COMPLETADA EXITOSAMENTE")
println("=" * 70)
