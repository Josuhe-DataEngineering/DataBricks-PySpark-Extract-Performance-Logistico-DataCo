[README.md](https://github.com/user-attachments/files/31765526/README.md)
# Reporte Performance Logístico DataCo

Pipeline ELT en **Databricks (Lakehouse)** que automatiza el Reporte de Performance Logístico y Riesgo de Entrega por Región para el área de Operaciones/Supply Chain — reemplazando un proceso manual en Excel que tomaba entre 2 y 3 horas semanales a un analista de BI.

> Migración de una arquitectura on-premise (SSIS + SQL Server, capas LOAD → STAGE → DM) a una arquitectura Lakehouse en Databricks (RAW → Bronze → Silver → Gold), manteniendo el mismo diseño y propósito de negocio del proceso original.

---

## Contexto de negocio

El área de Operaciones no contaba con una vista consolidada del performance de entrega ni del riesgo de entregas tardías a nivel orden-región. La información se armaba manualmente cruzando exports del ERP, generando diferencias entre áreas que trabajaban con cortes de fecha distintos.

Este pipeline consolida esa información en un **reporte automático semanal**, entregado por correo a `BI_Rpt_PerformanceLogistico_DataCo`, que permite a Supply Chain y a las gerencias regionales (LATAM, Europe, USCA, Pacific Asia, Africa) priorizar envíos, elegir modos de transporte y gestionar reclamos por entregas tardías con datos consistentes y oportunos.

---

## Arquitectura

Lakehouse con patrón medallion, sobre **Unity Catalog** (Delta Lake) y orquestado con **Databricks Workflows**:

```
                    SHAREPOINT (Microsoft Graph API)
                              │
                              ▼
                    ┌───────────────────┐
                    │        RAW        │  /Volumes/workspace/raw/sharepoint/
                    │  CSV original      │  Archivo tal como llega, sin tocar
                    └─────────┬─────────┘
                              │
                              ▼
                    ┌───────────────────┐
                    │       BRONZE       │  brz_dataco.sharepoint
                    │  Datos crudos      │  + trazabilidad de origen
                    │  ingeridos         │
                    └─────────┬─────────┘
                              │
                    ┌─────────┴─────────┐
                    ▼                   ▼
            ┌───────────────┐   ┌───────────────┐
            │    SILVER      │   │    SILVER      │  slv_dataco.operaciones
            │  Dimensiones   │   │    Hechos      │  Limpio, tipado, dedup
            │ (clientes,     │   │ (pedidos,      │
            │  productos,    │   │  detalle)      │
            │  ubicaciones)  │   │                │
            └───────┬───────┘   └───────┬───────┘
                    │                   │
                    └─────────┬─────────┘
                              ▼
                    ┌───────────────────┐
                    │  SILVER ENRIQUECIDO │  Joins materializados:
                    │                     │  pedido+cliente, detalle+producto
                    └─────────┬─────────┘
                              │
                              ▼
                    ┌───────────────────┐
                    │        GOLD        │  gld_dataco.operaciones
                    │   7 KPIs de        │  Historizado por corte semanal
                    │   performance      │
                    │   logístico        │
                    └─────────┬─────────┘
                              │
                              ▼
                    ┌───────────────────┐
                    │      REPORTING      │  Excel (4 pestañas) + envío
                    │                     │  por correo automático
                    └───────────────────┘
```

### Capas del Lakehouse

| Capa | Propósito | Estructura |
|---|---|---|
| **RAW** | Archivo original descargado desde SharePoint, sin transformar | `/Volumes/workspace/raw/sharepoint/` |
| **Bronze** | Datos crudos ingeridos, conservando origen y trazabilidad | `brz_dataco.sharepoint` |
| **Silver** | Datos limpios, tipados, deduplicados y modelados | `slv_dataco.operaciones` |
| **Gold** | Indicadores y datos listos para consumo/reporting | `gld_dataco.operaciones` |

---

## Stack tecnológico

- **Databricks** (Free Edition) — compute serverless, Unity Catalog
- **PySpark** — transformaciones ELT
- **Delta Lake** — formato transaccional en todas las capas
- **Databricks Workflows** — orquestación (equivalente al paquete maestro SSIS)
- **Microsoft Graph API** (client credentials flow) — extracción desde SharePoint
- **Databricks Secrets** — credenciales (tenant_id, client_id, client_secret)

---

## Nomenclatura

Cada capa vive en su propio catálogo de Unity Catalog, con convención de nombres consistente:

| Capa | Patrón | Ejemplo |
|---|---|---|
| Bronze | `brz_{origen}.{fuente}.{categoria}_{tabla_origen}` | `brz_dataco.sharepoint.raw_dataco_supply_chain` |
| Silver | `slv_{origen}.{área}.{tipo}_{entidad}` | `slv_dataco.operaciones.md_clientes`, `hd_pedidos` |
| Gold | `gld_{origen}.{área}.{modelo}_{tipo}_{entidad}` | `gld_dataco.operaciones.kpi_on_time_delivery` |

Prefijos de tipo en Silver: `md_` (maestro/dimensión — clientes, productos, ubicaciones), `hd_` (hechos/detalle — pedidos, pedido_detalle).

---

## Trazabilidad end-to-end

Un `id_ejecucion` se genera **una sola vez**, en la task `00_prepare_pipeline_control`, y se propaga por todas las capas — permite reconstruir, para cualquier fila de Gold, exactamente qué corrida del pipeline la produjo.

| Capa | Columnas de trazabilidad |
|---|---|
| Bronze | `fecha_inicio_corte`, `fecha_fin_corte`, `fecha_carga`, `id_ejecucion`, `origen`, `archivo_origen` |
| Silver | `fecha_inicio_corte`, `fecha_fin_corte`, `fecha_carga`, `id_ejecucion` |
| Gold | `fecha_inicio`, `fecha_fin`, `fecha_carga`, `id_ejecucion` |

---

## Orquestación (Databricks Workflow)

```
00_prepare_pipeline_control
        │
        ▼
01_ingest_sharepoint_bronze
        │
        ├──────────┬──────────┬──────────┬──────────┐
        ▼          ▼          ▼          ▼          ▼
  silver_clientes  productos  ubicaciones  pedidos  pedido_detalle
        │                                    │           │
        └──────────────┬─────────────────────┘           │
                        ▼                                 │
          silver_enr_pedido_cliente          silver_enr_detalle_producto
                        │                                 │
                        └────────────────┬────────────────┘
                                         ▼
                              04_gold_kpis (7 indicadores)
                                         │
                                         ▼
                          Reporting: Excel + envío por correo
```

- **`00_prepare_pipeline_control`**: lee el corte vigente desde `pipeline_control`, genera `id_ejecucion`, publica ambos vía Task Values.
- **`01_ingest_sharepoint_bronze`**: descarga el CSV desde SharePoint (Graph API), filtra por el corte, escribe a RAW y Bronze.
- **`02_silver_dimensiones` / `02_silver_hechos`**: limpieza, tipado y deduplicación desde Bronze (histórico completo acumulado).
- **`03_silver_enr_*`**: joins materializados (pedido+cliente, detalle+producto) para simplificar los cálculos de Gold.
- **`04_gold_kpis`**: un notebook por indicador, calcula sobre Silver e historiza en Gold por corte (`DELETE` + `append`, nunca `overwrite` — necesario para poder comparar cada semana contra la anterior).

---

## Indicadores (KPIs)

| # | Indicador | Fórmula | Grano |
|---|---|---|---|
| 1 | **On-Time Delivery %** | Σ(órdenes con days_real ≤ days_scheduled) / Σ(total órdenes) | Región + modo de envío |
| 2 | **Late Delivery Risk %** | Σ(late_delivery_risk = 1) / Σ(total órdenes) | Región + modo de envío |
| 3 | **Shipping Variance** | days_for_shipping_real − days_for_shipment_scheduled | Región (promedio) |
| 4 | **Profit Margin %** | (order_profit_per_order / sales) × 100 | Línea de detalle |
| 5 | **Revenue por Cliente** | Σ(sales) / COUNT(DISTINCT customer_id) | Segmento de cliente |
| 6 | **Beneficio por Orden** | sales − (product_price × order_item_quantity − order_profit_per_order) | Línea de detalle |
| 7 | **Lead Time Promedio** | AVG(days_for_shipping_real) | Modo de envío |

### Tablas Gold

```
gld_dataco.operaciones
    ├── kpi_on_time_delivery
    ├── kpi_late_delivery_risk
    ├── kpi_shipping_variance
    ├── kpi_profit_margin
    ├── kpi_revenue_por_cliente
    ├── kpi_beneficio_por_orden
    └── kpi_lead_time_promedio
```

### Reglas de negocio

- Se excluyen órdenes con `order_status = 'CANCELED'`. Las `SUSPECTED_FRAUD` se incluyen pero se marcan visualmente en el reporte.
- Las órdenes con `delivery_status = 'Shipping canceled'` se muestran en el detalle pero no entran al cálculo de On-Time Delivery % ni Lead Time.
- Agrupaciones de mercado: `GLOBAL` = LATAM + Europe + USCA + Pacific Asia + Africa; `AMERICAS` = LATAM + USCA.
- La fecha de corte del reporte es siempre el **domingo anterior** a la ejecución (job corre los lunes).

---

## Reporte final (Excel, 4 pestañas)

| Pestaña | Contenido |
|---|---|
| **Resumen Ejecutivo** | 1 fila por mercado, 7 KPIs totalizados + variación vs. semana anterior |
| **Detalle Región** | 1 fila por región + modo de envío, con los 7 indicadores |
| **Detalle Orden** | Grano máximo — para decisiones puntuales y gestión de reclamos |
| **Alertas** | Órdenes con `late_delivery_risk = 1` y performance de región < 70% |

Semaforización condicional:

| Condición | Formato |
|---|---|
| On-Time Delivery % < 70% | Celda roja |
| On-Time Delivery % 70%–89.9% | Celda amarilla |
| Late Delivery Risk % > 55% | Celda roja |
| Profit Margin % < 0 (pérdida) | Fila completa rojo claro |
| Shipping Variance > 5 días | Texto rojo |

Envío automático por correo a `BI_Rpt_PerformanceLogistico_DataCo`, todos los lunes.

---

## Configuración

1. **Secret scope** (credenciales de Microsoft Graph API):
   ```bash
   databricks secrets create-scope sharepoint
   databricks secrets put-secret sharepoint tenant_id
   databricks secrets put-secret sharepoint client_id
   databricks secrets put-secret sharepoint client_secret
   ```
2. Importar los notebooks al Workspace de Databricks respetando la estructura de carpetas.
3. Crear el Job en **Jobs & Pipelines** con las tasks y dependencias descritas en la sección de Orquestación.
4. Ajustar los widgets de catálogo/esquema si el naming difiere del default (`brz_dataco`, `slv_dataco`, `gld_dataco`, esquema `operaciones`).

---

## Estructura del repositorio

```
00_metadata/
    00_prepare_pipeline_control
01_ingest_sharepoint_bronze/
    01_ingest_sharepoint_bronze
02_silver_dimensiones/
    silver_clientes
    silver_productos
    silver_ubicaciones
02_silver_hechos/
    silver_pedidos
    silver_pedido_detalle
03_silver_enr_pedido_cliente/
03_silver_enr_detalle_producto/
04_gold_kpis/
    kpi_on_time_delivery
    kpi_late_delivery_risk
    kpi_shipping_variance
    kpi_profit_margin
    kpi_revenue_por_cliente
    kpi_beneficio_por_orden
    kpi_lead_time_promedio
```

---

## Origen de los datos

Dataset **DataCo Smart Supply Chain for Big Data Analysis** (Kaggle), publicado en una biblioteca de SharePoint corporativo para simular el flujo de un entorno real:

- `DataCoSupplyChainDataset.csv` — dataset principal (+180,000 registros, 53 columnas)
- `DescriptionDataCoSupplyChain.csv` — diccionario de datos
- `tokenized_access_logs.csv` — clickstream (complementario, fuera del alcance del reporte principal)

Fuente: https://www.kaggle.com/datasets/shashwatwork/dataco-smart-supply-chain-for-big-data-analysis
