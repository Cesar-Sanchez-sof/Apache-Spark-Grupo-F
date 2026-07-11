-- ============================================================
-- MODELO RELACIONAL - CATÁLOGO BIBLIOGRÁFICO NACIONAL
-- ============================================================
-- Diseño normalizado en Tercera Forma Normal (3NF)
-- Curso:  Big Data y Analítica de Datos
-- Actividad: Análisis Inteligente del Catálogo Bibliográfico
--            Nacional utilizando Apache Spark y Grafos
-- ============================================================
-- JUSTIFICACIÓN DEL DISEÑO:
--   1. Se separan las entidades principales (Áreas, Subáreas,
--      Editoriales, Autores, Libros, Bibliotecas) para eliminar
--      la redundancia de texto presente en el archivo plano CSV.
--   2. Las relaciones Muchos-a-Muchos (Libro↔Autor y Libro↔Biblioteca)
--      se resuelven mediante tablas intermedias (LIBRO_AUTORES
--      y LIBRO_BIBLIOTECAS), cada una con su clave primaria compuesta.
--   3. La columna "BIBLIOTECA" no existe en el archivo original;
--      se genera de forma determinista mediante hashing del ISBN
--      para distribuir los registros entre 3 bibliotecas virtuales.
-- ============================================================

-- ····························································
-- TABLA: AREAS
-- Descripción: Áreas principales del conocimiento.
--   Ejemplos: ADMINISTRACIÓN, DERECHO, INGENIERÍA, MEDICINA
-- ····························································
CREATE TABLE AREAS (
    id_area        INT           PRIMARY KEY AUTO_INCREMENT,
    nombre_area    VARCHAR(100)  NOT NULL UNIQUE
    -- UNIQUE evita que se registren áreas duplicadas
);

-- ····························································
-- TABLA: SUBAREAS
-- Descripción: Especialidades dentro de cada área.
--   Ejemplo: Dentro de ADMINISTRACIÓN →
--            "ADMINISTRACIÓN DE OPERACIONES / PRODUCCIÓN"
-- Relación: Cada subárea pertenece a exactamente un área (1:N)
-- ····························································
CREATE TABLE SUBAREAS (
    id_subarea       INT           PRIMARY KEY AUTO_INCREMENT,
    nombre_subarea   VARCHAR(250)  NOT NULL,
    id_area          INT           NOT NULL,
    -- Clave foránea: referencia a la tabla AREAS
    FOREIGN KEY (id_area) REFERENCES AREAS(id_area)
        ON DELETE RESTRICT    -- No se puede eliminar un área que tenga subáreas
        ON UPDATE CASCADE     -- Si se modifica el id_area, se actualiza aquí
);

-- ····························································
-- TABLA: EDITORIALES
-- Descripción: Catálogo de casas editoriales (publishers).
--   Ejemplos: PEARSON, MCGRAW-HILL, TRILLAS, MACRO
-- ····························································
CREATE TABLE EDITORIALES (
    id_editorial       INT           PRIMARY KEY AUTO_INCREMENT,
    nombre_editorial   VARCHAR(200)  NOT NULL UNIQUE
);

-- ····························································
-- TABLA: AUTORES
-- Descripción: Catálogo de autores de libros.
--   En el dataset original el autor se almacena como apellido
--   (e.g., "HEIZER", "JOHNSON", "FLORES").
-- ····························································
CREATE TABLE AUTORES (
    id_autor       INT           PRIMARY KEY AUTO_INCREMENT,
    nombre_autor   VARCHAR(200)  NOT NULL UNIQUE
);

-- ····························································
-- TABLA: BIBLIOTECAS
-- Descripción: Catálogo de bibliotecas / librerías del sistema.
--   Se generan 3 bibliotecas virtuales para el análisis de grafos.
-- ····························································
CREATE TABLE BIBLIOTECAS (
    id_biblioteca        INT           PRIMARY KEY AUTO_INCREMENT,
    nombre_biblioteca    VARCHAR(100)  NOT NULL UNIQUE
);

-- ····························································
-- TABLA: LIBROS
-- Descripción: Tabla central del catálogo bibliográfico.
--   - Clave primaria natural: ISBN (International Standard Book Number)
--   - Cada libro pertenece a una editorial y a una subárea.
-- ····························································
CREATE TABLE LIBROS (
    isbn            VARCHAR(25)   PRIMARY KEY,      -- ISBN como PK natural
    titulo          VARCHAR(350)  NOT NULL,
    edicion         VARCHAR(15),                     -- Puede ser numérica o texto (e.g., "4R", "DL")
    anio            INT,                             -- Año de publicación
    id_editorial    INT           NOT NULL,
    id_subarea      INT           NOT NULL,
    -- Claves foráneas
    FOREIGN KEY (id_editorial) REFERENCES EDITORIALES(id_editorial)
        ON DELETE RESTRICT
        ON UPDATE CASCADE,
    FOREIGN KEY (id_subarea) REFERENCES SUBAREAS(id_subarea)
        ON DELETE RESTRICT
        ON UPDATE CASCADE
);

-- ····························································
-- TABLA: LIBRO_AUTORES (tabla intermedia / tabla puente)
-- Descripción: Resuelve la relación Muchos-a-Muchos entre
--   LIBROS y AUTORES.
--   - Un libro puede tener múltiples autores.
--   - Un autor puede escribir múltiples libros.
-- ····························································
CREATE TABLE LIBRO_AUTORES (
    isbn       VARCHAR(25)  NOT NULL,
    id_autor   INT          NOT NULL,
    -- Clave primaria compuesta: combinación única de (isbn, id_autor)
    PRIMARY KEY (isbn, id_autor),
    FOREIGN KEY (isbn)     REFERENCES LIBROS(isbn)
        ON DELETE CASCADE,
    FOREIGN KEY (id_autor) REFERENCES AUTORES(id_autor)
        ON DELETE CASCADE
);

-- ····························································
-- TABLA: LIBRO_BIBLIOTECAS (tabla intermedia / tabla puente)
-- Descripción: Resuelve la relación Muchos-a-Muchos entre
--   LIBROS y BIBLIOTECAS.
--   - Almacena el stock disponible y el precio de venta (PVP)
--     para cada libro en cada biblioteca específica.
-- ····························································
CREATE TABLE LIBRO_BIBLIOTECAS (
    isbn              VARCHAR(25)    NOT NULL,
    id_biblioteca     INT            NOT NULL,
    stock             INT            DEFAULT 0,       -- Cantidad de ejemplares en esa biblioteca
    pvp_s             DECIMAL(10,2),                   -- Precio de venta al público en soles (S/)
    -- Clave primaria compuesta
    PRIMARY KEY (isbn, id_biblioteca),
    FOREIGN KEY (isbn)           REFERENCES LIBROS(isbn)
        ON DELETE CASCADE,
    FOREIGN KEY (id_biblioteca)  REFERENCES BIBLIOTECAS(id_biblioteca)
        ON DELETE CASCADE
);

-- ============================================================
-- DATOS DE REFERENCIA: Inserción de las 3 bibliotecas virtuales
-- ============================================================
-- Estas bibliotecas se generan mediante hashing del ISBN:
--   abs(hash(ISBN)) % 3 == 0 → Biblioteca A
--   abs(hash(ISBN)) % 3 == 1 → Biblioteca B
--   abs(hash(ISBN)) % 3 == 2 → Biblioteca C
-- ============================================================
INSERT INTO BIBLIOTECAS (nombre_biblioteca) VALUES
    ('Biblioteca A'),
    ('Biblioteca B'),
    ('Biblioteca C');

-- ============================================================
-- FIN DEL MODELO RELACIONAL
-- ============================================================
