from fastapi import FastAPI
from utils import fetch_order_data, fetch_customer_data, fetch_sales_data, add_customer_data, update_user, delete_user
from pydantic import BaseModel
from typing import Optional

class OrderResponse(BaseModel):
    order_id: str
    customer_id: int
    order_date: str
    total_order_value: float
    acquisition_channel: str

class CustomerResponse(BaseModel):
    customer_id: int
    customer_name: str
    customer_email: str
    customer_country: str
    total_orders: Optional[int] = None
    total_spent: Optional[float] = None
    last_order_date: Optional[str] = None
    avg_order_value: Optional[float] = None

class SalesResponse(BaseModel):
    order_date: str
    total_revenue: float
    total_orders: int
    avg_order_value: float
    top_channel: str

class AddUserRequest(BaseModel):
    customer_id: int
    customer_name: str
    customer_email: str
    customer_country: str

app = FastAPI()

@app.get("/orders/{order_id}", response_model=OrderResponse) 
def get_order(order_id: str):
    return fetch_order_data(order_id)

@app.get("/customers/{customer_id}", response_model=CustomerResponse)
def get_customer(customer_id: int):
    return fetch_customer_data(customer_id)

@app.get("/sales", response_model=SalesResponse)
def get_sales(order_date: str):
    return fetch_sales_data(order_date)

@app.post("/customers", response_model=AddUserRequest)
def add_user(request: AddUserRequest):
    add_customer_data(request.customer_id, request.customer_name, request.customer_email, request.customer_country)
    return request


@app.put("/customers/{customer_id}", response_model=AddUserRequest)
def update_user_(customer_id: int, request: AddUserRequest):
    update_user(customer_id, request.customer_name, request.customer_email, request.customer_country)
    request.customer_id = customer_id
    return request

@app.delete("/customers/{customer_id}", response_model=AddUserRequest)
def delete_user_(customer_id: int):
    delete_user(customer_id)
    return AddUserRequest(
        customer_id=customer_id,
        customer_name="",
        customer_email="",
        customer_country=""
    )