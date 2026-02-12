# Analisis-de-Datos-DataShop-Expandido
simulacion de analisis de datos de una empresa de retail ficticia DataShop. ETL completo y Dashboard  
<br> <br>
En este proyecto se presenta lo realizado en el Ejercicio Final de una capacitacion de Analisis de Datos, cubriendo desde el 
modelado de datos hasta la visualización en Power BI.  <br>
Fuentes de Datos: Generados a partir de un Script de Python tomando como guía los CVS dados en el ejercicio.  <br>
Rango de fechas para las ventas va desde 1/2020 hasta el 10/2025. El mes de octubre de 2025 es el mes adicional (ventas_add.csv). 
<br>

<img width="919" height="742" alt="Captura de pantalla (73)" src="https://github.com/user-attachments/assets/b9a94c88-4f8b-41ff-88eb-c988aa1be498" />





<br><br>

El archivo orquestador corre todos los scripts de Python y SQL necesarios para extraer los datos y cargarlos en las tablas del DW en el siguiente orden: <br>
1. SQLQuerySTAGING.sql - Crea tablas STAGING <br>
2. SQLQueryINT.sql - Crea tablas INT <br>
3. SQLQueryCreateDW.sql - Crea tablas DW (Dimensiones y Fact) <br>
4. SQLQueryStoreProcedures.sql - Crea Stored Procedures <br>
5. extract_data.py - Extrae CSV -> STAGING <br>
6. load_STG_to_INT.py - Carga STAGING -> INT <br>
7. dw_loader.py - Carga INT -> DW <br>
<br>
La conexión al servidor y base de datos se maneja a partir de lo configurado en el Archivo  config.ini, que cada script de Python lee para poder conectarse a ella y hacer los cambios.<br>
<br>

Para la creación del informe interactivo en Power BI:  se usó direct Query para la conexión con la base de Datos. Siguió la creación de los gráficos detallados en la consigna. Se genero una tabla de Medidas en Power BI para agrupar a todas las que fueron creadas para poder realizar las mediciones pedidas. <br>
Una vez completado y verificado el funcionamiento de los gráficos para cada Hoja se desarrolló el diseño: Se construyeron Fondos SVGs en Figma para mejorar el orden visual de los gráficos y mantener una coherencia del diseño. Se eligió una paleta de colores violetas, azules y grises teniendo en cuenta que se trata de una empresa que comercializa tecnología.<br>


<img width="1306" height="723" alt="Captura de pantalla (64)" src="https://github.com/user-attachments/assets/4b112376-b274-4293-8c6b-1c64a574f5b6" />

<img width="1308" height="723" alt="Captura de pantalla (65)" src="https://github.com/user-attachments/assets/e3c11614-644a-4bfb-8abc-045ede16c872" />

<img width="1318" height="725" alt="Captura de pantalla (66)" src="https://github.com/user-attachments/assets/cc6df001-a216-43cf-a8bf-071d99ffca1d" />

<img width="1301" height="703" alt="Captura de pantalla (67)" src="https://github.com/user-attachments/assets/3780b659-db96-4f73-80db-6a82efc5cf8f" />

<img width="1311" height="731" alt="Captura de pantalla (68)" src="https://github.com/user-attachments/assets/8b694028-b40f-47f1-8173-e17f5ee88ad1" />

<img width="1317" height="735" alt="Captura de pantalla (69)" src="https://github.com/user-attachments/assets/9abc42b6-bd33-475f-b8bf-e44e23483a4e" />

<img width="1322" height="725" alt="Captura de pantalla (70)" src="https://github.com/user-attachments/assets/613d0da2-ca8c-4910-90ef-0f72ab8b322d" />

<img width="1311" height="725" alt="Captura de pantalla (71)" src="https://github.com/user-attachments/assets/7a0bb864-5c02-490f-ac78-d4e71fc87dfe" />
