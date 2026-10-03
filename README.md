<div align="center">

# 🏗️ DataCo Supply Chain · Data Lakehouse

### De un ETL on-premise con SSIS + Data Warehouse a un pipeline **ELT cloud end-to-end** sobre Databricks Lakehouse

De SharePoint al correo del área de negocio: ingesta, modelo dimensional con historial, observabilidad y un reporte ejecutivo que se envía solo.

<br>

![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&logo=databricks&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta_Lake-00ADD4?style=for-the-badge&logo=delta&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)
![Unity Catalog](https://img.shields.io/badge/Unity_Catalog-FF3621?style=for-the-badge&logo=databricks&logoColor=white)
![Microsoft Graph](https://img.shields.io/badge/Microsoft_Graph-0078D4?style=for-the-badge&logo=microsoft&logoColor=white)
![Slack](https://img.shields.io/badge/Alertas-Slack-4A154B?style=for-the-badge&logo=slack&logoColor=white)

</div>

---

## 🎯 En una línea

Tomé un reporte logístico que se armaba a mano cada semana cruzando exports en Excel (2–3 horas de un analista) y lo convertí en un **pipeline automático de punta a punta** sobre Databricks Lakehouse: toma los datos de SharePoint, los procesa en capas, calcula 7 indicadores de negocio y envía un Excel ejecutivo por correo, con trazabilidad y alertas en cada paso.

## ⭐ Lo que resuelve

- **Antes:** proceso manual, lento y con cifras distintas entre áreas según el corte.
- **Ahora:** una sola corrida reproducible, con reglas de negocio centralizadas, historial de cambios y entrega automática al área.

---

## 🧭 Pipeline de un vistazo

```
SharePoint ──▶ 🥉 Bronze ──▶ 🥈 Silver ──▶ 🥇 Gold ──▶ 📊 Excel + 📧 Correo
             (ingesta       (tipado,       (modelo       (reporte ejecutivo
              por corte)     dedupe)        estrella +     automático)
                                            KPIs)
                    └──── 📋 etl_log + 🚨 alertas Slack en cada task ────┘
```

| | |
|---|---|
| **Fuente** | Archivo en SharePoint Online, descargado vía Microsoft Graph |
| **Procesamiento** | PySpark + Spark SQL sobre Delta Lake |
| **Arquitectura** | Medallion: Bronze → Silver → Gold |
| **Orquestación** | Databricks Workflow (20 tasks, compute Serverless) |
| **Modelo** | Esquema estrella con **SCD2** en dimensiones y hechos clave |
| **Observabilidad** | Tabla `etl_log` + alerta a Slack ante cualquier fallo |
| **Entrega** | Excel de 4 pestañas enviado por correo (Microsoft Graph) |

---

## 🔧 Qué hace cada capa

### 🥉 Bronze — ingesta cruda
Descarga el archivo desde SharePoint y carga **solo el corte de fechas** definido en una tabla de control. Cada corte se agrega sin tocar los anteriores y el proceso es idempotente: volver a correrlo no duplica datos.

### 🥈 Silver — datos limpios y conformados
Tipado fuerte, normalización y deduplicación por llave de negocio. Separa el archivo de origen en entidades: clientes, productos, ubicaciones, pedidos y detalle de pedido.

### 🥇 Gold — modelo dimensional y KPIs
Esquema estrella listo para análisis:
- **5 dimensiones** (clientes y productos con historial SCD2; ubicaciones, fecha y modo de envío).
- **2 tablas de hechos** (pedidos con SCD2 y reglas de negocio; detalle de pedido).
- **Resumen por pedido** como base única de los indicadores.
- **4 vistas de KPIs:** Resumen Ejecutivo, Detalle por Región, Detalle por Orden (SCD2) y Alertas.

### 📊 Entrega a negocio
Un notebook genera el **Excel de 4 pestañas** con formato condicional tipo semáforo y lo **envía por correo** con una tabla resumen y un gráfico en el cuerpo del mensaje.

---

## 📐 Los 7 indicadores

On-Time Delivery % · Late Delivery Risk % · Shipping Variance · Profit Margin % · Revenue por Cliente · Beneficio por Orden · Lead Time Promedio

Con agrupaciones de negocio **GLOBAL** (todos los mercados) y **AMERICAS** (LATAM + USCA), y reglas de alcance: se excluyen los pedidos cancelados y se marcan los sospechosos de fraude.

---

## 🛡️ Confiabilidad y trazabilidad

- **Tabla `etl_log`:** cada notebook registra inicio, fin, filas procesadas y estado (`EN_PROCESO → EXITOSO / ERROR`), con un identificador único por ejecución del Workflow.
- **Manejo de errores:** cada task de negocio tiene su task de log que, ante un fallo, registra el detalle técnico de la excepción y **avisa a Slack**.
- **Control de corte:** una tabla de parámetros define la ventana a procesar; el reproceso del mismo corte es idempotente.

---

## 🗂️ Modelo dimensional

```
        dim_customers        dim_products
              │                    │
              ▼                    ▼
  dim_fecha ─▶ fact_orders ◀─ fact_order_details ◀─ dim_products
              ▲       ▲
        dim_locations  dim_shipping_mode
```

Las dimensiones de clientes y productos y la tabla de hechos de pedidos usan **SCD2**: conservan el historial, de modo que cada pedido puede analizarse con los atributos vigentes en su momento.

---

## 🛠️ Stack

| Área | Tecnología |
|---|---|
| Plataforma | Databricks (Free Edition) · Serverless |
| Procesamiento | PySpark · Spark SQL |
| Almacenamiento | Delta Lake |
| Catálogo | Unity Catalog |
| Ingesta y correo | Microsoft Graph API |
| Secretos | Databricks Secret Scopes |
| Alertas | Webhook de Slack |
| Reportería | openpyxl · matplotlib |

---

## 📁 Organización del repositorio

```
00_metadata/                  # control del pipeline (prepare / finalize) + logs
01_ingest_sharepoint_bronze/  # ingesta a Bronze + logs
02_silver_dimensiones/        # clientes, productos, ubicaciones + logs
02_silver_hechos/             # pedidos, detalle de pedido + logs
03_gold_dimensiones/          # 5 dimensiones + logs
03_gold_hechos/               # pedidos, detalle de pedido + logs
03_gold_kpis/                 # resumen por pedido, 4 KPIs, reporte + logs
```

Cada carpeta separa el notebook de negocio (`01_negocio/`) de su notebook de manejo de errores (`02_logs/`).

---

## 🔄 Workflow

Orquestado como un único Databricks Workflow de 20 tasks de negocio, cada una con su task de log de error. El grafo final corre en **abanico**: las capas que no dependen entre sí se ejecutan en paralelo.

```
prepare ─▶ ingest ─▶ ┌ 5 Silver ┐ ─▶ ┌ 7 Gold dim/hechos ┐ ─▶ order_summary ─▶ ┌ 4 KPIs ┐ ─▶ reporte ─▶ finalize
```

---

## 🧩 Contexto del proyecto

Proyecto de portafolio que **migra un proceso tradicional** (SQL Server Integration Services sobre Data Warehouse on-premise) a una **arquitectura Lakehouse en la nube**, conservando las reglas de negocio del reporte original. El dataset es público (*DataCo Smart Supply Chain*, Kaggle) y la infraestructura es real.

---

## 🗺️ Roadmap

Próximas mejoras de nivel productivo:

- [ ] Gobierno de datos: etiquetas de propiedad, enmascaramiento de datos sensibles y filtros por fila en Unity Catalog.
- [ ] Calidad de datos declarativa: tablas de reglas y de resultados, con constraints a nivel Delta.
- [ ] Dashboard de salud del pipeline sobre `etl_log`.
- [ ] Infraestructura como código con Databricks Asset Bundles y CI con GitHub Actions.
- [ ] Ingesta incremental con Auto Loader cuando la fuente pase a un almacenamiento de objetos.
- [ ] Capa de ML: usar la capa Gold como insumo de un modelo predictivo (retrasos de envío).

---

<div align="center">

**Josuhe Sosa Lara** · Data Engineer · Lima, Perú

*Dataset público · infraestructura real · construido de punta a punta sobre Databricks Lakehouse.*

</div>
