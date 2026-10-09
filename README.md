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
