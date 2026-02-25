from sqlalchemy import create_engine, text
from fastapi import HTTPException
from db import engine

def fetch_order_data(order_id: str):
    sql = text("""
        SELECT *
        FROM ANALYTICS_ORDERS
        WHERE order_id = :order_id
    """)

    try:
        with engine.connect() as conn:
            conn.execute(text("ALTER WAREHOUSE DBT_WH RESUME IF SUSPENDED"))
            result = conn.execute(sql, {"order_id": order_id})
            row = result.fetchone()
            if row is None:
                raise HTTPException(status_code=404, detail="Order not found")
            return dict(row._mapping)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

def fetch_customer_data(customer_id: int):
    sql = text("""
        SELECT 
            customer_id,
            customer_name, 
            customer_email,
            customer_country,
            total_orders,
            total_spent,
            last_order_date,
            avg_order_value
        FROM ANALYTICS_CUSTOMER_SUMMARY
        WHERE customer_id = :customer_id
    """)

    try:
        with engine.connect() as conn:
            conn.execute(text("ALTER WAREHOUSE DBT_WH RESUME IF SUSPENDED"))
            row = conn.execute(sql, {"customer_id": customer_id}).fetchone()
            if row is None:
                raise HTTPException(status_code=404, detail="Customer not found")
            return dict(row._mapping)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

def fetch_sales_data(order_date: str):
    sql = text("""
        with daily_sales as (
            SELECT 
                order_date,
                total_order_value,
                acquisition_channel
            FROM ANALYTICS_ORDERS
            WHERE order_date = :order_date
        )
        select 
            order_date,
            sum(total_order_value) as total_revenue,
            count(*) as total_orders,
            avg(total_order_value) as avg_order_value,
            (select acquisition_channel from daily_sales group by acquisition_channel order by count(*) desc limit 1) as top_channel
        from daily_sales 
        group by order_date

    """)

    try:
        with engine.connect() as conn:
            conn.execute(text("ALTER WAREHOUSE DBT_WH RESUME IF SUSPENDED"))
            row = conn.execute(sql, {"order_date": order_date}).fetchone()
            if row is None:
                raise HTTPException(status_code=404, detail="Sales data not found")
            return dict(row._mapping)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Add customer data to ANALYTICS_CUSTOMER_SUMMARY, not update
def add_customer_data(customer_id: int, customer_name: str, customer_email: str, customer_country: str):
    sql = text("""
        INSERT INTO ANALYTICS_CUSTOMER_SUMMARY (customer_id, customer_name, customer_email, customer_country)
        VALUES (:customer_id, :customer_name, :customer_email, :customer_country)
    """)
    try:
        with engine.connect() as conn:
            conn.execute(text("ALTER WAREHOUSE DBT_WH RESUME IF SUSPENDED"))
            conn.execute(sql, {"customer_id": customer_id, "customer_name": customer_name, "customer_email": customer_email, "customer_country": customer_country})
            conn.commit()
            return {"message": "Customer added successfully"}
    # Bring error if customer_id already exists
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

def update_user(customer_id: int, customer_name: str, customer_email: str, customer_country: str):
    sql = text("""
        UPDATE ANALYTICS_CUSTOMER_SUMMARY
        SET customer_name = :customer_name, customer_email = :customer_email, customer_country = :customer_country
        WHERE customer_id = :customer_id
    """)
    try:
        with engine.connect() as conn:
            conn.execute(text("ALTER WAREHOUSE DBT_WH RESUME IF SUSPENDED"))
            conn.execute(sql, {"customer_id": customer_id, "customer_name": customer_name, "customer_email": customer_email, "customer_country": customer_country})
            conn.commit()
            return {"message": "Customer updated successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

def delete_user(customer_id: int):
    sql = text("""
        DELETE FROM ANALYTICS_CUSTOMER_SUMMARY
        WHERE customer_id = :customer_id
    """)
    try:
        with engine.connect() as conn:
            conn.execute(text("ALTER WAREHOUSE DBT_WH RESUME IF SUSPENDED"))
            conn.execute(sql, {"customer_id": customer_id})
            conn.commit()
            return {"message": "Customer deleted successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))