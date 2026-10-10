# 🎧 Spotify End-to-End Data Engineering Pipeline
### Cloud-Native Medallion Architecture with Incremental CDC on Azure
____________________________________________________________________________________________________________________________________________________________________________________________________________________________

![image](https://github.com/user-attachments/assets/74d0b6de-caef-4318-b7fb-5c56c6c4478d)

____________________________________________________________________________________________________________________________________________________________________________________________________________________________
![image](https://github.com/user-attachments/assets/3de0bf13-0d75-4273-9447-151f0e37424f) ![image](https://github.com/user-attachments/assets/07b7b42c-4250-4c87-a2e1-d0135b63e227) ![image](https://github.com/user-attachments/assets/6cd79bfc-5443-4460-95bb-76c5a4afc2f7) ![image](https://github.com/user-attachments/assets/b2d83bc3-1835-469f-bb64-3d6e5cc8d5ed) ![image](https://github.com/user-attachments/assets/92434fff-99a4-4d98-8d7a-9e9743888826) ![image](https://github.com/user-attachments/assets/c45600b5-2556-4cd5-9ecd-c397cd21ca86)

### 🛠️ Nota de Arquitectura: El patrón CDC (Change Data Capture)
Este proyecto fue diseñado y orquestado al 100% desde cero. La pieza central no es solo mover datos, sino implementar un patrón de ingesta incremental CDC robusto en Azure Data Factory. En lugar de escanear tablas completas en cada ejecución (Full Load), el pipeline utiliza archivos JSON como "watermarks" (marcas de agua) para detectar y mover únicamente los registros nuevos o modificados, garantizando escalabilidad y bajo costo en Azure Data Lake Gen2.

### 🎧 Proyecto de Ingeniería de Datos End-to-End: Spotify con Azure

## 📌 Descripción del proyecto

Este proyecto implementa una solución de ingeniería de datos de extremo a extremo (End-to-End) utilizando Microsoft Azure, siguiendo un flujo de trabajo de extracción, ingesta y organización de datos inspirado en un caso de uso de Spotify.

El objetivo es construir una arquitectura de datos capaz de ingerir información desde una base de datos SQL de origen, gestionar cargas incrementales mediante un mecanismo de seguimiento de cambios y almacenar los datos en diferentes capas de un Data Lake.

La solución aplica principios fundamentales de la ingeniería de datos moderna, como la parametrización de pipelines, la automatización de procesos ETL/ELT, la gestión de metadatos y la arquitectura Medallion.

## 🎯 Objetivos

- Diseñar una arquitectura de datos en Microsoft Azure.

- Configurar la ingesta de datos desde Azure SQL Database.

- Implementar pipelines dinámicos y reutilizables en Azure Data Factory.

- Gestionar cargas incrementales mediante marcas de tiempo de control o watermarks.

- Organizar los datos en las capas Bronze, Silver y Gold.

- Aplicar buenas prácticas de almacenamiento, transformación y orquestación de datos.

- Construir una base para futuras soluciones de analítica y Business Intelligence.

____________________________________________________________________________________________________________________________________________________________________________________________________________________________
## 🏗️ Arquitectura

![image](https://github.com/user-attachments/assets/e8b01cd8-c310-4917-a130-d87f5a858073)

____________________________________________________________________________________________________________________________________________________________________________________________________________________________
### Descripción Detallada de Cada Capa

___________________________________________________________________________________________________________
#### 1. Capa de Fuentes de Datos
| Fuente | Descripción |
|--------|-------------|
| Spotify API | Proporciona metadatos musicales, registros de reproducción y otros datos JSON semiestructurados. ADF extrae datos de forma incremental a través del conector REST. |
| Azure SQL Database | Almacena datos estructurados del lado del negocio, identificando registros incrementales mediante columnas watermark (como UpdatedAt). |


**<mark>Decisión clave de diseño:</mark>** Las estrategias de incrementalidad de Spotify API y Azure SQL son diferentes. El lado de la API depende de ventanas de tiempo + paginación, mientras que el lado SQL depende de columnas watermark. Ambos son orquestados de manera unificada por ADF.

______________________________________________________________________________________________________________
#### 2. Capa de Control y Orquestación

**Azure Data Factory es el único motor de orquestación**, responsable de:

- **Disparadores programados:** Inicia Pipelines según un calendario (por ejemplo, cada hora/diariamente).

- **Lógica de carga incremental:** Lee la marca de agua de cdc.json mediante la actividad Lookup, construye consultas incrementales (WHERE cdc_column > last_cdc_value).

- **Parametrización dinámica:** Utiliza parámetros como schema, table, cdc_col para que un solo Pipeline sirva a múltiples tablas.

- **Ramas condicionales:** La actividad If determina si hay nuevos datos. Si no los hay, omite el Copy, ahorrando recursos de cómputo.

- **Actualización de marca de agua:** Después de completar el Copy, la actividad Script obtiene el valor máximo de CDC de la tabla fuente y lo escribe de vuelta en cdc.json para la siguiente ejecución.

_________________________________________________________________________________________________________________
**Mecanismo de control CDC**

  
![image](https://github.com/user-attachments/assets/7badc09c-a034-4259-bb5a-453862474570)

La ventaja de este patrón es cero procedimientos almacenados, cero dependencias externas, completamente impulsado por metadatos JSON.

___________________________________________________________________________________________________________________
#### 3. Capa de Almacenamiento — ADLS Gen2 (Tres Capas Medallion)

| Capa | Contenido | Formato | Política de Retención | Usuarios |
|------|-----------|---------|-----------------------|----------|
| Bronze | Datos brutos, retenidos tal cual	| JSON / CSV | Retención permanente | Ingenieros de datos, auditoría |
| Silver | Datos limpios, deduplicados, estandarizados y validados | Parquet / Delta | Reprocesable	| Analistas de datos, científicos de datos |
| Gold | Modelado dimensional, agregación, tablas orientadas al negocio | Delta Table | Optimizado para informes | Desarrolladores BI, usuarios de negocio |


- **Principio de la capa Bronze:** Sin limpieza, se retiene el estado original como "fuente única de verdad", permitiendo reprocesamiento en cualquier momento.

- **Principio de la capa Silver:** Esquema forzado, manejo de nulos, deduplicación, validación de reglas de negocio.

- **Principio de la capa Gold:** Modelo estrella o tablas anchas, optimizadas para rendimiento de consultas, compatible con DirectQuery.

____________________________________________________________________________________________________________________________
#### 4. Capa de Cómputo — Azure Databricks

Databricks asume **todas las transformaciones de Silver y Gold:**

- **Bronze → Silver:** PySpark lee JSON/CSV brutos, ejecuta lógica de limpieza (conversión de tipos, relleno de nulos, deduplicación, restricciones de esquema), escribe en formato Delta.

- **Silver → Gold:** Agregación de lógica de negocio (como estadísticas de reproducción por usuario, por canción), modelado dimensional, escritura como Delta Table.

- **Ventajas de Delta Lake:** Transacciones ACID, viaje en el tiempo (Time Travel), Schema Evolution, soporte para control de versiones de datos y rollback.

Databricks se integra de forma segura con ADLS Gen2 a través de Unity Catalog: crea un Access Connector, otorga el rol Storage Blob Data Contributor a la Managed Identity, y luego crea Storage Credential y External Location en Unity Catalog.

_____________________________________________________________________________________________________________________________
#### 5. Capa de Consumo

| Herramienta | Método de Conexión | Escenario de Uso |
|-------------|--------------------|------------------|
| Power BI | Databricks Connector, DirectQuery o Import | Informes de negocio, dashboards |
| Databricks SQL | SQL Warehouse | Análisis Ad-hoc, exploración de datos |


**<mark>Puntos clave de conexión de Power BI:</mark>** Usa el Server Hostname y HTTP Path del workspace de Databricks, autenticación mediante Personal Access Token o Azure AD. Para tablas Gold de alta frecuencia de actualización, se recomienda DirectQuery + Auto Page Refresh para obtener una experiencia casi en tiempo real.

_______________________________________________________________________________________________________________________________
#### 6. Gobernanza y Seguridad (fácil de omitir pero debe incluirse)

| Componente | Función |
|------------|---------|
| Unity Catalog | Gobernanza unificada de datos, control de acceso a nivel de columna/fila, linaje de datos |
| Azure Entra ID | Autenticación de identidad, Service Principal para pipelines automatizados |
| Azure Key Vault | Gestión de claves de Spotify API, cadenas de conexión SQL, tokens de Databricks |


**Principio de diseño de permisos:** Los usuarios de la capa Gold no deberían ver datos de Bronze/Silver. Esto se logra mediante roles de acceso de Unity Catalog o aislamiento en Workspaces separados.

_____________________________________________________________________________________________________________________________
## 🛠️ Stack Tecnológico Detallado

| Herramienta | Rol en el Proyecto | Justificación Arquitectónica |
|-------------|--------------------|------------------------------|
| Azure Data Factory | Orquestación & CDC |	Motor de ingesta. Implementa la lógica de Watermark (Lookup/JSON) para ingesta incremental parametrizada. |
| Azure Data Lake Gen2 | Storage (Data Lake) | Almacenamiento altamente escalable y barato. Contiene las capas Bronze/Silver/Gold en formato Parquet/Delta. |
| Azure SQL Database | Sistema Fuente (OLTP) | Simula la base de datos transaccional donde Spotify registra reproducciones y usuarios. |
| Azure Databricks | Procesamiento (ELT) | Motor de transformación. Aplica Upserts (Merge) y agregaciones complejas usando Spark/PySpark. |
| Delta Lake | Formato de Archivo | Utilizado en Silver/Gold. Permite ACID transactions, Time Travel y UPSERTS que Parquet no soporta. |
| Power BI | Visualización | Consumo directo de la capa Gold para métricas operativas de Spotify. |

____________________________________________________________________________________________________________________________________________________________________________________________________________________________
### 🧠 La Magia: Patrón CDC en Azure Data Factory

El mayor valor de este proyecto no es el "Select *", sino cómo resuelve la **ingesta incremental** sin usar herramientas de CDC nativas de SQL Server.

**El flujo lógico dentro de ADF es:**

- **Lookup Activity (last_cdc):** Lee un archivo cdc.json en el Data Lake que contiene el último timestamp o ID procesado (ej: 1901).
- **Copy Data Activity:** Ejecuta un query dinámico a Azure SQL:

    SELECT * FROM DimUser WHERE UserId > '@{activity('last_cdc').output.value[0].cdc}'

- **Script Activity (max_cdc):** Obtiene el nuevo máximo de la tabla origen:

    SELECT MAX(UserId) as cdc FROM DimUser

- **Copy Data Activity (update_last_cdc):** Sobrescribe el archivo cdc.json con el nuevo max_cdc para la próxima ejecución.
- **If Condition:** Si dataRead == 0 (no hay datos nuevos), elimina el archivo Parquet vacío para no ensuciar el Data Lake. 

____________________________________________________________________________________________________________________________________________________________________________________________________________________________
## ✨ Aspectos Técnicos Destacados

**1. Pipeline Parametrizado Dinámico**

Utilizando conjuntos de datos parametrizados y generación dinámica de consultas, se logra que un solo pipeline procese datos de múltiples entidades de Spotify, evitando el mantenimiento de pipelines duplicados.

**2. Carga Incremental de Datos (CDC)**

El pipeline mantiene la marca de agua de CDC mediante la actividad Lookup last_cdc, cargando solo datos nuevos en lugar de sobrescribir todo. Lógica de expresión:

Código:

        @if(empty(pipeline().parameters.from_date),
            activity('last_cdc').output.value[0].cdc,
            pipeline().parameters.from_date)

**¿Cómo funciona?**
- Se consultan los metadatos asociados a la tabla que se va a procesar.
- Se recupera el último valor de control registrado.
- Se ejecuta una consulta parametrizada para extraer los registros que superan ese valor.
- Se calcula el máximo valor de control de los datos procesados.
- Se actualiza el metadato para la siguiente ejecución.
- Se contempla una condición para gestionar ejecuciones en las que no existen nuevos registros.

**<mark>Beneficios</mark>**
- Reduce la necesidad de volver a cargar todos los registros.
- Permite mantener un punto de control entre ejecuciones.
- Facilita la reutilización de pipelines para distintas tablas.
- Mejora la eficiencia de los procesos de ingesta.
- Proporciona una base para desarrollar procesos de integración de datos más escalables.

💡**Nota técnica:** el mecanismo de watermark implementado en este proyecto permite gestionar cargas incrementales. No debe confundirse automáticamente con CDC nativo de SQL Server, que utiliza mecanismos específicos para registrar cambios.

**3. Funcionalidades Avanzadas de Delta Lake**

- **Control de versiones de datos:** Cada escritura genera automáticamente una versión

- **Viaje en el tiempo:** Permite consultar cualquier versión histórica

- **Tombstoning:** Gestión de eliminación lógica

**4. Procesamiento de Escenarios en Tiempo Real**

Soporte para lectura de datos a través de Service Principal, cubriendo escenarios de autenticación y gestión de permisos en entornos de producción.

____________________________________________________________________________________________________________________________________________________________________________________________________________________________
## 📂 Arquitectura Medallion (Data Lake Structure)

### 🥉 Bronze Layer (Raw CDC Data - Append Only)

- **Objetivo:** Ingesta rápida del "delta" de datos desde Azure SQL.
- **Formato:** Parquet.
- **Ruta:** bronze/{Table}_cdc/{Table}_{timestamp}.parquet
- **Acción:** ADF escribe aquí usando Copy Activity. No se borra ni se/ modifica historial.

### 🥈 Silver Layer (Cleaned & Deduplicated - UPSERT)

- **Objetivo:** Datos limpios, sin duplicados y enriquecidos. Aplica SCD Type 1 (Overwrite) or Type 2 (History).
- **Formato:** Delta Lake (Para permitir operaciones MERGE).
- **Acción:** Databricks lee los nuevos Parquets de Bronze y ejecuta un MERGE INTO la tabla Silver existente, actualizando usuarios que cambiaron de ciudad o insertando nuevos.

### 🥇 Gold Layer (Aggregated for BI)

- **Objetivo:** Tablas de hechos y dimensiones listas para consumo.
- **Formato:** Delta Lake.
- **Acción:** Databricks agrupa las reproducciones por artista, canción y fecha para generar métricas de popularidad y tendencia.

__________________________________________________________________________________________________________________________________________________________________________________________________
## 📁 Estructura del Proyecto

![image](https://github.com/user-attachments/assets/f1506c44-426e-4e3f-80c8-a46aeef94d2d)


__________________________________________________________________________________________________________________________________________________________________________________________________
## 🚀 Quick Start

**Requisitos Previos**
- Suscripción de Azure.
- Instancia de Azure Data Factory.
- Espacio de trabajo de Azure Databricks.
- Cuenta de almacenamiento ADLS Gen2.

**Paso 1:** Clonar el Repositorio

git clone https://github.com/AllGoHer/spotify-project.git

**Paso 2:** Crear Infraestructura en Azure (Portal)

1. Inicia sesión en portal.azure.com.
2. Crea un Grupo de Recursos llamado RG-Spotify.
3. Dentro de RG-Spotify, crea los siguientes recursos:
4. Storage Account: spotiproject (Habilitar Hierarchical namespace para ADLS Gen2).
5. Dentro del Storage, ve a Containers y crea 3 contenedores: bronze, silver, gold.
6. Azure SQL Database: sql-spotify (Configura un servidor y contraseña).
7. Data Factory: df-SpotifyProject (Al crearlo, selecciona la opción de Git integration para vincularlo a tu repositorio local clonado).

**Paso 3: Poblar la Base de Datos (Azure SQL)**

1. Ve a tu Azure SQL Database (sql-spotify) en el portal.
2. Abre el Query Editor y autentícate con tu usuario/contraseña.
3. Abre el archivo local sql/spotify_initial_load.sql.
4. Copia todo el contenido, pégalo en el Query Editor y ejecútalo.
5. Resultado: Se crearán las tablas DimUser, DimArtist, DimTrack, DimDate y FactListening con miles de filas simuladas.

**Paso 4: Configurar CDC (Change Data Capture) en ADLS**

Para que el pipeline sepa dónde dejó de leer la última vez, necesita los archivos "watermark" (marcas de agua).

1. Abre Azure Storage Explorer (o usa el portal) y conéctate a tu Storage Account spotiproject.
2. Navega al contenedor bronze.
3. Crea una carpeta llamada cdc dentro de bronze.
4. Copia los archivos empty.json y cdc.json de la carpeta raíz de tu repositorio clonado a la carpeta bronze/cdc/.
5. Crea las subcarpetas para las dimensiones dentro de bronze: DimUser_cdc, DimArtist_cdc, DimTrack_cdc, DimDate_cdc.
6. Importante: Copia el archivo cdc.json dentro de cada una de esas subcarpetas recién creadas.


**Paso 5: Configurar Azure Data Factory (Linked Services)**

Si usaste la integración de Git en el Paso 2, los pipelines y datasets ya están importados. Solo necesitas apuntarlos a tus recursos:

1. Abre Azure Data Factory Studio (Manage -> Author).
2. Ve a Manage -> Linked services.
3. Abre el Linked Service de Azure SQL y actualiza el nombre del servidor y la contraseña con los del Paso 2. Haz "Test connection" y guarda.
4. Abre el Linked Service de ADLS Gen2 y actualiza el nombre del Storage Account. Haz "Test connection" y guarda.


**Paso 6: Ejecutar el Pipeline (Debug)**
1. En Data Factory, ve a la pestaña Author y abre el pipeline incremental_ingestion.
2. Haz clic en Debug (Depurar).
3. En la ventana emergente de parámetros, ingresa lo siguiente:
    - schema: dbo
    - table: DimUser
    - cdc_col: Updated_at

Haz clic en OK.

Espera a que todas las actividades del pipeline se pongan en verde.

Ve a la salida de la actividad last_cdc y verifica que devuelva un valor (ej: 1901).

**Paso 7: Verificar los Datos en el Data Lake**
1. Abre Azure Storage Explorer.
2. Navega a spotiproject -> bronze -> DimUser.
3. Deberás ver un archivo Parquet nuevo con timestamp (ej: DimUser_20261008_120000.parquet).
4. ¡Felicidades! Tu ingesta incremental CDC está funcionando.

**Paso 8: Transformar en Databricks (Silver & Gold)**

1. Crea un clúster en Azure Databricks.
2. Sube los notebooks de la carpeta databricks/ a tu Workspace.
3. Ejecuta 01_silver_upsert.py adjuntando tu clúster (esto convertirá Bronze Parquet a Silver Delta con lógica MERGE).
4. Ejecuta 02_gold_aggregations.py (esto creará las tablas finales para Power BI).

__________________________________________________________________________________________________________________________________________________________________________________________________
## DESARROLLO Y EVIDENCIAS.

Iniciamos ingresando a Azure / grupo de recursos y creamos un nuevo recurso.

![image](https://github.com/user-attachments/assets/a195fcbf-fd73-48d6-9082-4f917b6d2354)

![image](https://github.com/user-attachments/assets/99f20a86-934d-4e92-b3a9-dec5e0f0cebe)

![image](https://github.com/user-attachments/assets/4d252228-8452-4e83-90b1-4c0d8eda54f2)

Ahora, vamos a grupo de recursos y actualizamos, para poder seleccionar el nuevo grupo creado.

![image](https://github.com/user-attachments/assets/ea10afd2-a66e-48e8-92d6-e0e327a7209c)

Luego, ingresamos a RG-Spotify y, hacemos click en crear y seleccionamos storage account.

![image](https://github.com/user-attachments/assets/44120d6f-5cac-4e39-b4de-f4245d605ec8)

![image](https://github.com/user-attachments/assets/3da96f7a-7ffe-4352-bd8f-b85d6715c8a4)

![image](https://github.com/user-attachments/assets/bf958238-c93f-4d7e-ab6c-d386fc9ee557)

![image](https://github.com/user-attachments/assets/6cfa6939-1f76-4d4a-a231-30cf91e81e20)

![image](https://github.com/user-attachments/assets/c0f04023-2070-4b88-ae31-6106e134a58e)

![image](https://github.com/user-attachments/assets/7a2acdda-a261-4052-b5b1-a83df6f2cc5f)

Luego, esperas un minuto hasta que se complete la  implementación. 

![image](https://github.com/user-attachments/assets/b7ad4786-1d35-415b-a3d3-04c9c7f7749e)

Ahora, volvemos a grupos de recursos y seleccionamos RG-Spotify y, creamos un nuevo grupo de recurso.

![image](https://github.com/user-attachments/assets/32065988-f8a2-4569-a49b-9b8452c43826)

![image](https://github.com/user-attachments/assets/c20d0000-c7c1-4218-8555-50d94074921c)

![image](https://github.com/user-attachments/assets/a854e6c7-d3a9-4d4d-aa34-d883405a1b5b)

![image](https://github.com/user-attachments/assets/80267d60-45b9-4d85-8600-366dacf5520a)

![image](https://github.com/user-attachments/assets/b91de073-6168-4c36-86c2-5d9c5807eb3e)

![image](https://github.com/user-attachments/assets/048d2018-c66f-4e8b-a1ed-d0d867b9c794)

![image](https://github.com/user-attachments/assets/1f734b0b-571c-4cb9-9208-74fdd497ac88)

Ahora, nos vamos a RG-Spotify y, entramos a la cuenta de almacenamiento spotiproject y luego a containers

![image](https://github.com/user-attachments/assets/83dfbe6f-6bcd-4f4c-b8e8-94cd6d8b4664)

![image](https://github.com/user-attachments/assets/9689577b-926a-43cb-8d5e-97ef7836b123)

Hacemos click en agregar contenedor y lo llamamos bronce 

![image](https://github.com/user-attachments/assets/65c4fd2b-63f4-4432-b8a7-59eb1e620829)

Y hacemos de la misma manera para crear las capas silver y gold.

![image](https://github.com/user-attachments/assets/fa44a413-73cc-4d0f-9426-4ba07bc94d49)

Ahora, nos vamos a df-SpotifyProyecto y entramos a Azure Data Factory.

![image](https://github.com/user-attachments/assets/8a3d07a1-737d-4182-b4aa-e99e4cf15d20)

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

video1: https://youtu.be/x01_498suPc

video2: https://youtu.be/KmfgqQUKgz4

VIDEO3: https://youtu.be/J0pjGjG1Zp8

video4: 


