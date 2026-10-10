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

![image](https://github.com/user-attachments/assets/9fe57c81-4a87-4a5c-955a-97bc056d49f4)

Luego, vamos a nuestro github y creamos un nuevo repositorio (Spotify_Project)


![image](https://github.com/user-attachments/assets/d94b3fe2-b179-481e-99be-5708c181f40e)


![image](https://github.com/user-attachments/assets/4e6794d8-5875-446b-a27f-b464137787b2)


![image](https://github.com/user-attachments/assets/1dbf3b89-a752-4bcc-96f6-a6f1b210b748)

Ahora, volvemos a Azure Data Factory y lo vinculamos con el repositorio creado.

![image](https://github.com/user-attachments/assets/63ad9253-f682-4434-871e-d7a99052d646)

![image](https://github.com/user-attachments/assets/1cca6e6b-61f3-41d0-b982-35c2f76b7658)

Se abrirá una ventana para solicitar autorización, el cual debemos aceptar.

![image](https://github.com/user-attachments/assets/743460fc-b6c3-4552-90f7-a983c85cdc01)

![image](https://github.com/user-attachments/assets/74e40fd7-b57d-4011-ac99-8867f595fecc)

Seleccionamos Spotifi-Project y aplicamos.

![image](https://github.com/user-attachments/assets/2d2baf55-7c8c-45c3-883e-8f3503659f31)

![image](https://github.com/user-attachments/assets/9bb56382-2541-47b1-baad-1392ef20444e)

Nos vamos ahora a autor y creamos una nueva rama de git.

![image](https://github.com/user-attachments/assets/75ed4a6d-8455-48a1-81ba-1e0f6df45f6a)

![image](https://github.com/user-attachments/assets/8bb7bdf8-4985-4fb1-a31a-6d7e0e31670d)

![image](https://github.com/user-attachments/assets/949ff120-f99f-4259-bdce-1cbdf0152992)

Esto te ayuda identificar tu desarrollo, puesto que habrá muchas personas trabajando en equipo en el proyecto.

![image](https://github.com/user-attachments/assets/3fe06033-ccf8-4ce2-982d-de35c0afd83c)

Luego, nos vamos a portal Azure y creamos un nuevo recurso 

![image](https://github.com/user-attachments/assets/c54282d8-a4b9-48dc-ae8f-a77039585e95)

Buscamos Azure SQL

![image](https://github.com/user-attachments/assets/ccd10598-4bf0-47d3-a061-5a0c35fdf981)

![image](https://github.com/user-attachments/assets/e9c46c08-699e-4790-98fc-31f2aca75442)

![image](https://github.com/user-attachments/assets/6f1a2610-b707-4fa9-b71a-2e2f3740afde)

![image](https://github.com/user-attachments/assets/6c4e15ee-6806-4e5a-8793-94fe356bd075)

![image](https://github.com/user-attachments/assets/42f6db07-0fc7-4180-be98-7abfc427f1df)

![image](https://github.com/user-attachments/assets/730a9773-e6e6-4769-942c-ad481d3651e5)

![image](https://github.com/user-attachments/assets/5520d7c9-16c2-4933-af71-a23a6b377f85)

![image](https://github.com/user-attachments/assets/51fc49b4-9fd9-42d6-94bb-9f31b3e0b0b7)

![image](https://github.com/user-attachments/assets/b91f83db-5688-4d1b-a96b-f3fe9232d535)

![image](https://github.com/user-attachments/assets/13f49da8-ec8a-408e-b08d-b247ea6c055d)

Luego, regresamos a RG-Spotify y hacemos click en la base de datos sql

![image](https://github.com/user-attachments/assets/740d66fe-0a1f-4a34-8358-27c79e5e24e9)

Nos vamos a query editor

![image](https://github.com/user-attachments/assets/e67e161f-1a49-4177-aecc-2774d032a4ea)

![image](https://github.com/user-attachments/assets/0552073d-b3d3-48f4-9784-a74691f83636)

Si en caso no entra, puedes acceder a través de Autenticación de Microsoft Entra. 

![image](https://github.com/user-attachments/assets/e4cf1410-bf7c-4ee3-b6c7-11cc153f4a22)

![image](https://github.com/user-attachments/assets/216cff3f-3e4b-4952-878c-2dab5184500c)

Ahora, ingresan a mi github (AllGoHer) al siguiente archivo: spotify_project/source_scripts/spotify_initial_load.sql y copian todo el archivo y lo pegan en la consulta.

Copiar:

![image](https://github.com/user-attachments/assets/522394bb-0d8c-4439-a471-286127af163a)

Pegar:

![image](https://github.com/user-attachments/assets/b7781ac8-8b14-4188-9344-372542ab851b)

Y ejecutamos.

![image](https://github.com/user-attachments/assets/7eb3c157-908f-488e-82d6-8437e3cb391a)

Y se crearan las siguientes tablas.

![image](https://github.com/user-attachments/assets/f19f738b-ce6c-4392-9312-c2094da31ccb)

Volvemos a Azure Data Factory a la pestaña de autor

![image](https://github.com/user-attachments/assets/4a27a6be-7b5f-4a1d-bc23-0a777314a08c)

Luego, llenamos todos los datos solicitados

![image](https://github.com/user-attachments/assets/a9923452-03d5-4b5a-858e-8b78a9fc5730)

![image](https://github.com/user-attachments/assets/a7d16330-ee73-4ab7-85ea-bc3b41b0735a)

A veces con SQL Autenticación hay problemas, espere un par de minutos y vuelva intentar la conexión y tendrá éxito. Luego de ello haga click en crear.

![image](https://github.com/user-attachments/assets/a90ff834-13a9-4dc7-8945-95b06e4c8d85)

![image](https://github.com/user-attachments/assets/1f0f7603-b5d2-41bd-83c8-c49d796e76bc)

Ahora, nuevamente hacemos click en nuevo y creamos Azure Data Lake Gen2.

![image](https://github.com/user-attachments/assets/4a8ae26b-a10c-46c8-b84f-bd363f5a5a0d)

![image](https://github.com/user-attachments/assets/8dc7e5ae-c70b-43ae-8298-894e54479145)

Hacemos la prueba de autenticación.

![image](https://github.com/user-attachments/assets/4db835f2-1f91-46eb-96d2-e34ffe7c2656)

![image](https://github.com/user-attachments/assets/83a52ddc-0514-4963-9aa0-bc4fb49d5f0e)

Ahora, vamos a autor y creamos un pipeline.

![image](https://github.com/user-attachments/assets/81beae2c-6430-4bc0-9303-629fa6581b64)

![image](https://github.com/user-attachments/assets/e70ff192-0aec-4abc-a76a-6b52d29f498d)

Luego en actividades escribimos copiar y arrastramos el cuadro de texto copiar datos al lienzo 

![image](https://github.com/user-attachments/assets/4528b6b0-8137-4fc0-9513-6be4e3b9d1df)

![image](https://github.com/user-attachments/assets/6fad92cd-7a6f-4abe-be5d-4c3ce4fae173)

![image](https://github.com/user-attachments/assets/ea409752-5cf2-4759-bf53-9e5703a674b2)

![image](https://github.com/user-attachments/assets/66f9e700-644d-4010-8855-a4c41317ed03)

![image](https://github.com/user-attachments/assets/34ea374c-7cdc-4c2b-a37e-07ac39f259ad)

Modificamos el nombre solo borrando la palabra Table1.

![image](https://github.com/user-attachments/assets/57b0b6ac-4f5b-42a8-8d8d-1c64d665937a)

En utilizar consulta seleccionamos consulta y escribimos la consulta.

![image](https://github.com/user-attachments/assets/44ec7532-aac3-46d7-92e4-272908b54f0f)

Ahora, aplicaremos CDC para identificar y registrar los cambios (creaciones, actualizaciones y eliminaciones) realizados en la base de datos. En lugar de copiar o escanear una tabla completa cada vez que se necesita actualizar la información, el CDC detecta únicamente los registros nuevos o modificados y los transmite casi en tiempo real.
Para ello utilizares archivos json y agregaremos parámetros.
Entonces, hacemos click en el lienzo en blanco para llegar a parámetros y hacemos click en nuevo tres veces, uno esquema, otro para tabla y otro para cdc.

![image](https://github.com/user-attachments/assets/cfa159d7-37f5-44c9-a8a1-9f267b77f2f7)

![image](https://github.com/user-attachments/assets/8c00a173-ce8e-4a0c-a213-7b30c1afe87f)

Ahora en Azure, nos vamos a almacenamiento de datos (spotiproject) dentro de él, nos vamos a contenedores y luego a bronce.

![image](https://github.com/user-attachments/assets/45d819d6-5d78-4e67-a538-9ed2d0ce44b2)

Luego en bronce agregamos un directorio, el cual llamaremos cdc.

![image](https://github.com/user-attachments/assets/ec804d20-7c10-4182-ac60-eacafc3b51dd)

Y guardamos y, se creará un archivo json vacio.

![image](https://github.com/user-attachments/assets/c110779d-757b-4223-baaa-6e48e78a0d9d)

![image](https://github.com/user-attachments/assets/19cccf9d-be24-4fad-8d33-dfc3707a98f2)

Luego, descargamos el archivo empty.json de mi github (AllGoHer) 
https://github.com/AllGoHer/spotify_project/blob/main/empty.json
y lo cargamos en el archivo cdc de la capa bronce.

![image](https://github.com/user-attachments/assets/f1c2a8df-5594-4189-9128-fd29920288f1)

![image](https://github.com/user-attachments/assets/8b077ca0-455d-4d91-87a0-473af2a0d118)

Ahora, también descargamos de mi github el archivo cdc.json y lo cargamos.

![image](https://github.com/user-attachments/assets/3afa4117-6f8f-417a-a388-81bd5bf4a71d)

Luego, regresamos a azure data factory y en actividades escribimos búsqueda y lo arrastramos al lienzo. 

![image](https://github.com/user-attachments/assets/461b0e96-6a6b-4bc7-928c-b2f6934a997f)

Le cambiamos el nombre por last_cdc.

![image](https://github.com/user-attachments/assets/985a1671-2da4-4b9d-b57b-c325db893e06)

Ahora, los vamos a la pestaña de configuración y desmarcamos el recuadro que dice “solo la primera fila” y creamos un conjunto de datos.

![image](https://github.com/user-attachments/assets/9e51251c-ac97-4fc8-b42d-6dbd79800a7e)

![image](https://github.com/user-attachments/assets/78127909-9fce-4d0f-83a6-2cd9a516951d)

![image](https://github.com/user-attachments/assets/41e15b12-11f9-444e-bd4a-914fd6ebdd07)

![image](https://github.com/user-attachments/assets/9b7b9050-2385-4dc5-a08f-c3ad9539cbd6)

![image](https://github.com/user-attachments/assets/6cef0049-1463-46e5-b493-0ff01c0a7d99)

![image](https://github.com/user-attachments/assets/780aa671-759b-485b-bdf6-d136ead0a9c7)

![image](https://github.com/user-attachments/assets/ee8ff19e-38e9-433d-956a-3231d415c384)

Luego, damos click en avanzado, después en abrir este conjunto de datos 

![image](https://github.com/user-attachments/assets/01b19ab8-9b4e-4076-b569-9edd17d5a057)

![image](https://github.com/user-attachments/assets/4ac6ff20-cc36-4cb6-bcb7-63d9c49c461d)

Hacemos click en parámetros y luego 3 veces a nuevo para crear los parámetros de container, folder y file.

![image](https://github.com/user-attachments/assets/0985ef58-9120-4b5c-8428-37644f874995)

Ahora, vamos a conexión

![image](https://github.com/user-attachments/assets/fc5bfd4f-dd64-433d-a207-35c30cd1b46b)

Luego de hacer click en contenido dinámico, hacemos click en contenedor.

![image](https://github.com/user-attachments/assets/4d0daa14-e21d-4d7e-b722-a932f1823ecf)

![image](https://github.com/user-attachments/assets/db2db01d-b6e7-4d06-860f-c35d3e59dcb5)

Y luego aceptamos.

![image](https://github.com/user-attachments/assets/43cbd9cc-7005-4205-837f-512fbd9acbcd)

Luego hacemos click en el siguiente casillero y luego en contenido dinámico y, agregamos folder.

![image](https://github.com/user-attachments/assets/e7b41326-d871-488d-a530-994d691af422)

![image](https://github.com/user-attachments/assets/581b83b0-5d30-4342-bf49-0c1eddcd1c4c)

Y luego aceptamos.

![image](https://github.com/user-attachments/assets/3d04b70f-d5df-4965-ab64-356427c43df9)

Y de la misma forma para el siguiente casillero y agregamos file.

![image](https://github.com/user-attachments/assets/723322de-df39-443d-8708-9599a023ca66)

Luego, hacemos click en guardar y en la ventana emergente aceptamos.

![image](https://github.com/user-attachments/assets/3dac24a0-2f71-475e-99ec-1059e2cbbf4f)

Hacemos click en incremental.

![image](https://github.com/user-attachments/assets/e754ddba-1329-4db4-af12-fd7b24198f68)

![image](https://github.com/user-attachments/assets/27d10b85-afcd-46e6-8bb2-e7e66e66c80d)

Esto nos ayuda a manejar mas de 100 archivos diferentes de una forma dinámica y no uno por uno. Entonces agregaremos los archivos correspondientes

![image](https://github.com/user-attachments/assets/2da2d9f9-2c57-401a-badc-98bfc24b42dc)

Que vendría hacer lo mismo que esto en Azure.

![image](https://github.com/user-attachments/assets/ab09a3ba-3c05-47fb-b0f1-54c66a8ab677)

Ahora, hacemos click en copiar data del lienzo.

![image](https://github.com/user-attachments/assets/55fb6f5d-1596-498c-a290-aad3d2690131)

Hacemos click en la pestaña de general y desactivamos la actividad.

![image](https://github.com/user-attachments/assets/4794a069-63ef-4bd2-ab04-ab9895eac502)

Ahora, depuramos.

![image](https://github.com/user-attachments/assets/d8a7e9d3-106e-48b4-90e6-6d652946eb47)

Luego aceptamos.

![image](https://github.com/user-attachments/assets/a15df4b9-bafa-4bf7-a44d-6a73fe0948ea)

![image](https://github.com/user-attachments/assets/1b502ae5-71c3-4154-9ca1-12ddec33969f)

Para ver que todo está funcionando bien, en last_cdc hacemos click en la flechita de salida y tendremos que ver que se haya cargado 1901.

![image](https://github.com/user-attachments/assets/8cf06059-b9b5-4f13-b09c-ab5058252358)

![image](https://github.com/user-attachments/assets/d4e6f9ba-79aa-4614-b492-69ae0bcb1162)

Ahora, usaremos ese resultado en mi actividad, así es que, lo uniremos a copiar datos y lo activaremos.

![image](https://github.com/user-attachments/assets/41f6d5c8-2c88-4f28-a83d-3c955e12bb02)

Ahora hacemos click en origen y, luego hacemos click dentro del recuadro de consultas y activara abajo un texto azul que dice agregar contenido dinámico, hacemos click en él y se abrirá una ventana de generador de expresiones de canalización.

![image](https://github.com/user-attachments/assets/894265fd-e3d2-4169-9211-30ddda086467)

![image](https://github.com/user-attachments/assets/dcee955d-d049-460a-83e6-97b080d7f520)

Y generamos el siguiente código.

Código:

        SELECT * FROM @{pipeline().parameters.schema}.@{pipeline().parameters.table} WHERE @{pipeline().parameters.cdc_col} > 


Luego, vamos a la pestaña de Resultados de la actividad y en el código ponemos comillas y seleccionamos last_cdc.


![image](https://github.com/user-attachments/assets/9bf2eb98-1748-45c5-863e-54c78fbad85a)

Hasta que quede el código de la siguiente manera:

Código:

        SELECT * FROM @{pipeline().parameters.schema}.@{pipeline().parameters.table} WHERE @{pipeline().parameters.cdc_col} > '@{activity('last_cdc').output.value[0].cdc}'


![image](https://github.com/user-attachments/assets/75ba4655-5dbe-4fe3-a48b-2233197387f4)

Y finalmente damos click en aceptar.

![image](https://github.com/user-attachments/assets/83a88bff-e581-40a7-9588-30e1a1b92877)

Luego, nos vamos al Receptor (Sink) y hacemos click en nuevo para crear un archivo de formato parquet.

![image](https://github.com/user-attachments/assets/581f1b77-995c-43cd-aaf0-cf26e07fbd33)

![image](https://github.com/user-attachments/assets/1adb909b-1ab4-423b-803d-6307b984662b)

![image](https://github.com/user-attachments/assets/bef1c55c-57d6-4e14-9020-501fcb36a88d)

![image](https://github.com/user-attachments/assets/09062205-4524-4614-bf29-20ec1a77bd3e)

En avanzado hacemos click en las letras azules conjunto de datos.

![image](https://github.com/user-attachments/assets/16d0de3c-31be-4446-a0f6-d69689242a1d)

Seleccionamos la pestaña parámetros y creamos 3 nuevos parámetros.

![image](https://github.com/user-attachments/assets/342ad725-30c9-48d3-9c23-d4297b5a67a2)

Nos vamos a conexión y en ruta de acceso agregamos de forma dinámica en cada casillero lo siguiente: container, folder y file.

Ejemplo:

![image](https://github.com/user-attachments/assets/078c42f5-97c3-4498-89ea-9ee217603dc6)

Debe quedar así:

![image](https://github.com/user-attachments/assets/08107fa0-1794-4b36-bd39-bbb4eeb93b3b)

Luego, damos click en guardar (que esta en la esquina superior izquierda) y aceptar.

![image](https://github.com/user-attachments/assets/35d63cae-5445-4d87-a2b6-22c36a0671d7)

![image](https://github.com/user-attachments/assets/257bbc2d-81f3-4aec-bcf8-ae953dfb4f9e)

Ahora, hacemos click en la pestaña superior que dice incremental_ingestion y en actividades escribimos establecer variable y lo arrastramos al lienzo.

![image](https://github.com/user-attachments/assets/3c09bacc-3ff5-48cd-92b6-d1335b37e213)

![image](https://github.com/user-attachments/assets/ae18c93e-2e2b-4ca4-90a2-cb0bb9ed8a79)

Y cambiamos el nombre a current y lo unimos a copiar datos.

![image](https://github.com/user-attachments/assets/8f828516-b512-4104-b0fc-119151aa7825)

Hacemos click en la parte blanca del lienzo y nos vamos a la pestaña de variables y, creamos una nueva variable con el nombre current.

![image](https://github.com/user-attachments/assets/48c86b59-dde1-4f95-b20f-f9bcc5f3f19e)

Regresamos a establecer variables en la pestaña de configuración, en el recuadro de nombre seleccionamos current; luego hacemos click en el cuadro de valor y luego a contenido dinámico. 

![image](https://github.com/user-attachments/assets/f7a916ed-ae17-4f45-903f-d67a6a7e3fbc)

![image](https://github.com/user-attachments/assets/c01156f7-1ce1-4857-a231-304733d9eef0)

![image](https://github.com/user-attachments/assets/efcec812-35a1-447d-b83e-7b5536b0023a)

Ahora, seleccionamos copiar datos y en Receptor (Sink) pasamos los siguientes datos:

Container: bronze

Folder: Users

File:   agregaremos contenido dinámico.

Primero, seleccionamos table.

![image](https://github.com/user-attachments/assets/5c8edabe-73c7-49f7-8999-50e7669e0f02)

Segundo, agregamos concatenar variables actuales.

Código:

        @concat(pipeline().parameters.table,'_',variables('current'))

![image](https://github.com/user-attachments/assets/13a25fd5-ca30-4100-8585-b9b09d195cb6)

Y damos click en aceptar.

![image](https://github.com/user-attachments/assets/4f8ef59e-a40c-4c63-8599-09a0c00bcc57)

Luego depuramos y en la ventana emergente agregamos los siguientes datos.

![image](https://github.com/user-attachments/assets/79a6623b-2b96-4431-a366-74e5dd0dba48)

Ahora aceptamos.

![image](https://github.com/user-attachments/assets/d04f6d2a-aecc-4b7f-9cf4-bdd9f58c419c)

🎥VIDEO: [Spotify1_carga_de_datos](https://youtu.be/x01_498suPc)


Luego, verificamos en Azure en el archivo de la capa bonze que haya cargado el archivo.

![image](https://github.com/user-attachments/assets/f5f1ad08-87d8-4a23-9171-e986dbf88929)

![image](https://github.com/user-attachments/assets/1f10dc28-e70b-4e7e-801f-132304965323)


Si luego vuelves hacer una carga incremental, veras un nuevo archivo, pero con los datos actualizados.

Ahora, creamos una nueva actividad de copiar datos en el lienzo llamado update_last_cdc.

![image](https://github.com/user-attachments/assets/4d481d8d-4ece-493c-9973-f8f492cf82c4)

Unimos las dos actividades de copiar datos.

![image](https://github.com/user-attachments/assets/c9cfabce-bad9-4ab1-8fb1-239cd8ee7cd5)

![image](https://github.com/user-attachments/assets/b11c477e-32b8-4ed2-b5d8-0e1b95c33901)

![image](https://github.com/user-attachments/assets/d6133ee5-03ae-46df-b5b7-d71005616337)

En valor seleccionamos agregar contenido dinámico. 

![image](https://github.com/user-attachments/assets/ede12336-e21a-4df8-a650-8fefe6bc57f1)

Agregamos cualquier contenido dinámico para que no escoja la ruta predeterminada como es $$FILEPATH. 

Y luego aceptamos.

![image](https://github.com/user-attachments/assets/aceb1a7f-621c-4255-8667-0a764c759234)

Esta acción permitirá modificar la data respecto al último cdc.

Ahora, vamos a la pestaña del Receptor (Sink) y pasamos los siguientes datos.


![image](https://github.com/user-attachments/assets/75c4a4d0-cbe5-4cd3-8f0e-b57de44c0908)

Ahora, desconectamos las dos actividades de copiar datos para poner una actividad de scripts intermedio, el cual se unirá a la primera actividad de copiar datos.

![image](https://github.com/user-attachments/assets/cdbcd645-ef5a-44da-9bc1-d8daecede33e)

Y cambiamos el nombre por max_cdc.

![image](https://github.com/user-attachments/assets/1a1119ee-4995-45b2-ad9b-aa8f9d689383)

![image](https://github.com/user-attachments/assets/b8e4bcf3-2890-43d3-80fc-76cc6656c43e)

![image](https://github.com/user-attachments/assets/ca4db924-1095-4168-b521-dab79f956b7d)

Código:

        SELECT MAX(@{pipeline().parameters.cdc_col}) as cdc FROM @{pipeline().parameters.schema}.@{pipeline().parameters.table}


Ahora, seleccionamos el segundo copiar data y lo desactivamos.

![image](https://github.com/user-attachments/assets/a12320b9-d760-4aa1-a0b3-6a21047151c2)

Luego, depuramos.

![image](https://github.com/user-attachments/assets/c64e43da-6fe6-49f8-bda0-d18d84c555d6)

![image](https://github.com/user-attachments/assets/20e66395-75c9-4d08-86f1-c7e465a78102)

Y aceptamos.

🎥VIDEO: [Spotify2-max_cdc](https://youtu.be/KmfgqQUKgz4)


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

video4: https://youtu.be/1vTa1Bhssas

video5: https://youtu.be/SlFDUD8QTpE


