{% docs __overview__ %}

# E-commerce Analytics Pipeline

## Project Overview
This dbt project processes e-commerce event data from S3/Lambda and builds
dimensional models for analytics.

## Data Flow
1. **Source**: Events stream from Lambda → S3
2. **Staging**: Parse JSON, clean, and type-cast
3. **Intermediate**: Session aggregation and customer history
4. **Marts**: Fact tables and dimensions (star schema)
5. **Analytics**: Pre-joined wide tables for BI tools

## Key Models

### Marts Layer (Star Schema)
For data engineers and advanced SQL users:
- `dim_customers` - Customer dimension with segmentation
- `fct_orders` - Order transactions
- `fct_order_items` - Line-item details

### Analytics Layer (Denormalized)
For BI tools and business users (no joins required):
- `analytics_orders` - Complete order view with customer + session context
- `analytics_order_items` - Line items with full order + customer data
- `analytics_customer_summary` - One-row-per-customer metrics with RFM scoring

## When to Use Which Layer?

**Use Marts** when:
- Writing custom SQL queries
- Building new dbt models
- Need normalized data for data integrity

**Use Analytics** when:
- Building dashboards in BI tools (Preset, Metabase, Tableau)
- Drag-and-drop analytics without SQL
- Need fast query performance for reporting

{% enddocs %}