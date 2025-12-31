"""
FastAPI Backend for The Drug Database (MySQL)
Config:
update DB_CONFIG with MySQL details
"""

# imports
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, Dict, List, Any
import mysql.connector
from mysql.connector import pooling
import os
import re
from datetime import datetime

app = FastAPI()

# CORS (Cross-Origin Resource Sharing) middleware
# Security mechanism to allow/restrict resources on a web server depending on the origin of the HTTP request
# Origins can be specified as a list of allowed domains (http/https, domain, and port)
# Credentials refers to cookies, auth headers, or TLS client certificates
# Methods refers to HTTP methods allowed for cross-origin requests
# Headers specifies HTTP headers the browser is allowed to use when making a request
# "*" means any is allowed
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Database configuration (specified for MySQL)
DB_CONFIG = {
    'host': os.environ.get('MYSQL_HOST', 'localhost'),
    'port': int(os.environ.get('MYSQL_PORT', 3306)),
    'user': os.environ.get('MYSQL_USER', 'root'),
    'password': os.environ.get('MYSQL_PASSWORD', 'password'),
    'database': os.environ.get('MYSQL_DATABASE', 'drug_database')
}

# Connection pool
# creates pre-established open database connections that the applications can reuse
# (connections are what transactions are executed on)
# Saves time from creating new connections for each request
connection_pool = pooling.MySQLConnectionPool(
    pool_name="drug_pool",
    pool_size=5,
    # Dictionary unpacking to pass DB_CONFIG items as keyword arguments
    **DB_CONFIG
)

# Dataset configurations
DATASET_CONFIGS = {
    "products": {
        "date_field": "approval_date",
        "filter_columns": ["appl_type", "form", "route", "type", "rld", "rs"],
        "search_fields": ["ingredient", "trade_name", "form", "route", "applicant", "type", "dosage"]
    },
    "exclusivity": {
        "date_field": "exclusivity_date",
        "filter_columns": ["appl_type", "form", "route", "exclusivity_code"],
        "search_fields": ["ingredient", "trade_name", "form", "route", "exclusivity_code"]
    },
    "patent": {
        "date_field": "submission_date",
        "filter_columns": ["appl_type", "form", "route", "drug_substance_flag", "drug_product_flag"],
        "search_fields": ["ingredient", "trade_name", "form", "route", "patent_no", "applicant"]
    },
    "sales": {
        "date_field": None,
        "filter_columns": ["route", "dosage", "manufacturer"],
        "search_fields": ["ingredient", "manufacturer", "route", "dosage", "ndc_number"]
	}
}


class SearchRequest(BaseModel):
    textQuery: Optional[str] = None
    filters: Optional[Dict[str, Any]] = None
    dateRanges: Optional[Dict[str, Dict[str, str]]] = None
    page: int = 1
    limit: int = 50
    sortBy: Optional[str] = None
    sortOrder: str = "ASC"


def get_connection():
    return connection_pool.get_connection()


def parse_text_query(query: str, dataset: str):
    """Parse natural language query into SQL conditions"""
    if not query or not query.strip():
        return [], []
    
    config = DATASET_CONFIGS.get(dataset, DATASET_CONFIGS["products"])
    date_field = config.get("date_field")
    search_fields = config["search_fields"]
    
    conditions = []
    params = []
    
    # Date patterns (only if date_field exists)
    if date_field:
        match = re.search(r'(?:approved|approval)\s+(?:after|since|from)\s+(\d{4})', query, re.IGNORECASE)
        if match:
            conditions.append(f"{date_field} >= %s")
            params.append(f"{match.group(1)}-01-01")
        
        match = re.search(r'(?:approved|approval)\s+(?:before|until|to)\s+(\d{4})', query, re.IGNORECASE)
        if match:
            conditions.append(f"{date_field} <= %s")
            params.append(f"{match.group(1)}-12-31")
        
        match = re.search(r'(?:approved|approval)\s+(?:in|during)\s+(\d{4})', query, re.IGNORECASE)
        if match:
            conditions.append(f"{date_field} BETWEEN %s AND %s")
            params.extend([f"{match.group(1)}-01-01", f"{match.group(1)}-12-31"])
    
    # Ingredient patterns
    match = re.search(r'(?:include|includes|containing|contain|with)\s+"([^"]+)"', query, re.IGNORECASE)
    if match:
        conditions.append("ingredient LIKE %s")
        params.append(f"%{match.group(1)}%")
    else:
        match = re.search(r'(?:include|includes|containing|contain|with)\s+(\w+)', query, re.IGNORECASE)
        if match:
            conditions.append("ingredient LIKE %s")
            params.append(f"%{match.group(1)}%")
    
    # Form patterns
    match = re.search(r'(?:form|dosage\s*form)\s+(?:is|=|:)?\s*"?(\w+)"?', query, re.IGNORECASE)
    if match:
        conditions.append("form LIKE %s")
        params.append(f"%{match.group(1)}%")
    
    # Route patterns
    match = re.search(r'(?:route|via)\s+(?:is|=|:)?\s*"?(\w+)"?', query, re.IGNORECASE)
    if match:
        conditions.append("route LIKE %s")
        params.append(f"%{match.group(1)}%")
    
    # Trade name patterns
    match = re.search(r'(?:trade\s*name|brand|called)\s+(?:is|=|:)?\s*"?([^"]+)"?', query, re.IGNORECASE)
    if match:
        conditions.append("trade_name LIKE %s")
        params.append(f"%{match.group(1).strip()}%")
    
    # Type patterns
    match = re.search(r'(?:type)\s+(?:is|=|:)?\s*"?(\w+)"?', query, re.IGNORECASE)
    if match:
        conditions.append("type = %s")
        params.append(match.group(1).upper())
    
    # General text search if no patterns matched
    if not conditions:
        stop_words = ['give', 'me', 'show', 'find', 'get', 'list', 'all', 'products', 'that', 'are', 'is', 'the', 'and', 'or', 'for', 'a', 'an', 'with']
        words = [w for w in query.split() if len(w) > 2 and w.lower() not in stop_words]
        
        for word in words:
            or_conditions = [f"{field} LIKE %s" for field in search_fields]
            conditions.append(f"({' OR '.join(or_conditions)})")
            params.extend([f"%{word}%" for _ in search_fields])
    
    return conditions, params


@app.get("/api/health")
def health_check():
    return {"status": "OK"}


@app.get("/api/stats")
def get_stats():
    conn = get_connection()
    cursor = conn.cursor()
    try:
        stats = {}
        for table in ["products", "exclusivity", "patent", "sales"]:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            stats[table] = cursor.fetchone()[0]
        return stats
    finally:
        cursor.close()
        conn.close()


@app.get("/api/filter-options/{dataset}")
def get_filter_options(dataset: str):
    if dataset not in DATASET_CONFIGS:
        raise HTTPException(status_code=400, detail="Invalid dataset")
    
    conn = get_connection()
    cursor = conn.cursor()
    try:
        config = DATASET_CONFIGS[dataset]
        options = {}
        
        for col in config["filter_columns"]:
            cursor.execute(f"SELECT DISTINCT {col} FROM {dataset} WHERE {col} IS NOT NULL AND {col} != '' ORDER BY {col} LIMIT 100")
            options[col] = [row[0] for row in cursor.fetchall()]
        
        # Add text filter fields with unique values
        text_filter_fields = {
            "products": ["ingredient", "trade_name", "appl_no", "number_of_approvals"],
            "exclusivity": ["ingredient", "trade_name", "appl_no", "exclusivity_code"],
            "patent": ["ingredient", "trade_name", "appl_no", "patent_use_code"],
            "sales": ["ingredient", "appl_no", "manufacturer", "number_of_sellers"]
        }
        
        if dataset in text_filter_fields:
            for col in text_filter_fields[dataset]:
                if col == "number_of_approvals" or col == "number_of_sellers":
                    # For numeric fields, get distinct values
                    cursor.execute(f"SELECT DISTINCT {col} FROM {dataset} WHERE {col} IS NOT NULL ORDER BY {col} LIMIT 100")
                else:
                    cursor.execute(f"SELECT DISTINCT {col} FROM {dataset} WHERE {col} IS NOT NULL AND {col} != '' ORDER BY {col} LIMIT 500")
                options[col] = [str(row[0]) for row in cursor.fetchall()]
        
        return options
    finally:
        cursor.close()
        conn.close()


@app.post("/api/search/{dataset}")
def search_dataset(dataset: str, request: SearchRequest):
    if dataset not in DATASET_CONFIGS:
        raise HTTPException(status_code=400, detail="Invalid dataset")
    
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        conditions = []
        params = []
        
        # Parse text query
        if request.textQuery:
            text_conditions, text_params = parse_text_query(request.textQuery, dataset)
            conditions.extend(text_conditions)
            params.extend(text_params)
        
        # Apply dropdown filters
        if request.filters:
            for field, values in request.filters.items():
                if isinstance(values, list) and values:
                    placeholders = ','.join(['%s'] * len(values))
                    conditions.append(f"{field} IN ({placeholders})")
                    params.extend(values)
                elif isinstance(values, str) and values.strip():
                    conditions.append(f"{field} LIKE %s")
                    params.append(f"%{values}%")
        
        # Apply date range filters
        if request.dateRanges:
            for field, range_val in request.dateRanges.items():
                if range_val.get("from"):
                    conditions.append(f"{field} >= %s")
                    params.append(range_val["from"])
                if range_val.get("to"):
                    conditions.append(f"{field} <= %s")
                    params.append(range_val["to"])
        
        where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        
        # Get total count
        cursor.execute(f"SELECT COUNT(*) as cnt FROM {dataset} {where_clause}", params)
        total = cursor.fetchone()['cnt']
        
        # Build sort
        order_clause = ""
        if request.sortBy:
            order = "DESC" if request.sortOrder.upper() == "DESC" else "ASC"
            order_clause = f"ORDER BY {request.sortBy} {order}"
        
        # Pagination
        offset = (request.page - 1) * request.limit
        
        # Get data
        cursor.execute(
            f"SELECT * FROM {dataset} {where_clause} {order_clause} LIMIT %s OFFSET %s",
            params + [request.limit, offset]
        )
        data = cursor.fetchall()
        
        # Convert dates to strings
        for row in data:
            for key, value in row.items():
                if isinstance(value, datetime):
                    row[key] = value.strftime("%Y-%m-%d")
                elif hasattr(value, 'strftime'):
                    row[key] = value.strftime("%Y-%m-%d")
        
        return {
            "data": data,
            "pagination": {
                "page": request.page,
                "limit": request.limit,
                "total": total,
                "totalPages": (total + request.limit - 1) // request.limit
            }
        }
    finally:
        cursor.close()
        conn.close()


@app.get("/api/schema/{dataset}")
def get_schema(dataset: str):
    schemas = {
        "products": [
            {"column_name": "appl_no", "data_type": "string"},
            {"column_name": "appl_type", "data_type": "string"},
            {"column_name": "ingredient", "data_type": "string"},
            {"column_name": "dosage", "data_type": "string"},
            {"column_name": "form", "data_type": "string"},
            {"column_name": "route", "data_type": "string"},
            {"column_name": "trade_name", "data_type": "string"},
            {"column_name": "applicant", "data_type": "string"},
            {"column_name": "strength", "data_type": "string"},
            {"column_name": "te_code", "data_type": "string"},
            {"column_name": "approval_date", "data_type": "date"},
            {"column_name": "rld", "data_type": "string"},
            {"column_name": "rs", "data_type": "string"},
            {"column_name": "type", "data_type": "string"},
            {"column_name": "applicant_full_name", "data_type": "string"},
            {"column_name": "number_of_approvals", "data_type": "integer"}
        ],
        "exclusivity": [
            {"column_name": "appl_no", "data_type": "string"},
            {"column_name": "appl_type", "data_type": "string"},
            {"column_name": "ingredient", "data_type": "string"},
            {"column_name": "dosage", "data_type": "string"},
            {"column_name": "form", "data_type": "string"},
            {"column_name": "route", "data_type": "string"},
            {"column_name": "trade_name", "data_type": "string"},
            {"column_name": "strength", "data_type": "string"},
            {"column_name": "exclusivity_code", "data_type": "string"},
            {"column_name": "exclusivity_date", "data_type": "date"}
        ],
        "patent": [
            {"column_name": "appl_no", "data_type": "string"},
            {"column_name": "appl_type", "data_type": "string"},
            {"column_name": "ingredient", "data_type": "string"},
            {"column_name": "dosage", "data_type": "string"},
            {"column_name": "form", "data_type": "string"},
            {"column_name": "route", "data_type": "string"},
            {"column_name": "trade_name", "data_type": "string"},
            {"column_name": "applicant", "data_type": "string"},
            {"column_name": "strength", "data_type": "string"},
            {"column_name": "patent_no", "data_type": "string"},
            {"column_name": "patent_expire_date_text", "data_type": "string"},
            {"column_name": "drug_substance_flag", "data_type": "string"},
            {"column_name": "drug_product_flag", "data_type": "string"},
            {"column_name": "patent_use_code", "data_type": "string"},
            {"column_name": "submission_date", "data_type": "date"}
        ],
        "sales": [
            {"column_name": "appl_no", "data_type": "string"},
            {"column_name": "ingredient", "data_type": "string"},
            {"column_name": "route", "data_type": "string"},
            {"column_name": "route_extended", "data_type": "string"},
            {"column_name": "dosage", "data_type": "string"},
            {"column_name": "manufacturer", "data_type": "string"},
            {"column_name": "strength", "data_type": "string"},
            {"column_name": "pack_quantity", "data_type": "integer"},
            {"column_name": "ndc_number", "data_type": "string"},
            {"column_name": "labeler_code", "data_type": "string"},
            {"column_name": "product_code", "data_type": "string"},
            {"column_name": "sales", "data_type": "decimal"},
            {"column_name": "packs", "data_type": "bigint"},
            {"column_name": "quantity", "data_type": "bigint"},
            {"column_name": "wac", "data_type": "decimal"},
            {"column_name": "price", "data_type": "decimal"},
            {"column_name": "number_of_sellers", "data_type": "integer"}
        ]
    }
    
    if dataset not in schemas:
        raise HTTPException(status_code=400, detail="Invalid dataset")
    
    return schemas[dataset]
