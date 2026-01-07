# demo_data_generator.py
"""
Cinta Beauty Manufacturing ERP - Demo Data Generator
Generates realistic manufacturing data for all 16 modules with proper production flow
"""

import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, date
import uuid
import random
import hashlib
import json
from typing import Dict, List, Optional
import os
from io import BytesIO

# Set page config
st.set_page_config(
    page_title="Admin & Data | The Verse Manufacturing ERP",
    page_icon="🏭",
    layout="wide"
)

# Set random seed for reproducibility
RANDOM_SEED = 42
np.random.seed(RANDOM_SEED)
random.seed(RANDOM_SEED)

# ==================== DATA SCHEMAS ====================
SCHEMAS = {
    # ========== PRODUCTION MODULE ==========
    "dim_formulations": {
        "required": {
            "formulation_id": "str",
            "product_name": "str",
            "product_category": "str",
            "version": "str",
            "status": "str",
            "created_date": "date",
            "expected_yield_percentage": "float",
            "production_cost_per_unit": "float",
            "target_cycle_time_hours": "float",
            "quality_standards": "str"
        }
    },
    
    "fact_production_batches": {
        "required": {
            "batch_id": "str",
            "formulation_id": "str",
            "production_date": "date",
            "planned_quantity": "int",
            "actual_quantity": "int",
            "yield_percentage": "float",
            "status": "str",
            "equipment_id": "str",
            "supervisor_id": "str",
            "quality_score": "float",
            "total_cost": "float",
            "completion_time_hours": "float"
        }
    },
    
    "dim_raw_materials": {
        "required": {
            "material_id": "str",
            "material_name": "str",
            "category": "str",
            "supplier_id": "str",
            "unit_of_measure": "str",
            "unit_cost": "float",
            "min_stock_level": "int",
            "current_stock": "int",
            "shelf_life_days": "int",
            "storage_requirements": "str"
        }
    },
    
    # ========== INVENTORY MODULE ==========
    "fact_inventory_transactions": {
        "required": {
            "transaction_id": "str",
            "transaction_date": "date",
            "transaction_type": "str",
            "sku": "str",
            "batch_id": "str",
            "quantity": "int",
            "unit_cost": "float",
            "total_value": "float",
            "location_id": "str",
            "reference_document": "str",
            "employee_id": "str"
        }
    },
    
    "dim_inventory_items": {
        "required": {
            "sku": "str",
            "item_name": "str",
            "category": "str",
            "unit_of_measure": "str",
            "current_stock": "int",
            "min_stock_level": "int",
            "max_stock_level": "int",
            "average_unit_cost": "float",
            "total_value": "float",
            "last_restock_date": "date",
            "expiry_date": "date",
            "storage_location": "str"
        }
    },
    
    # ========== SALES & POS MODULE ==========
    "fact_sales_transactions": {
        "required": {
            "transaction_id": "str",
            "transaction_date": "datetime",
            "customer_id": "str",
            "sku": "str",
            "quantity": "int",
            "unit_price": "float",
            "total_amount": "float",
            "discount_amount": "float",
            "tax_amount": "float",
            "net_amount": "float",
            "payment_method": "str",
            "payment_status": "str",
            "sales_channel": "str",
            "employee_id": "str"
        }
    },
    
    "dim_customers": {
        "required": {
            "customer_id": "str",
            "customer_name": "str",
            "customer_type": "str",
            "email": "str",
            "phone": "str",
            "address": "str",
            "city": "str",
            "country": "str",
            "customer_since": "date",
            "loyalty_points": "int",
            "total_purchases": "float",
            "last_purchase_date": "date"
        }
    },
    
    # ========== HR MODULE ==========
    "dim_employees": {
        "required": {
            "employee_id": "str",
            "first_name": "str",
            "last_name": "str",
            "email": "str",
            "phone": "str",
            "department": "str",
            "position": "str",
            "hire_date": "date",
            "salary": "float",
            "employment_type": "str",
            "supervisor_id": "str",
            "shift_pattern": "str",
            "active": "bool"
        }
    },
    
    "fact_attendance": {
        "required": {
            "attendance_id": "str",
            "employee_id": "str",
            "date": "date",
            "shift_start": "time",
            "shift_end": "time",
            "actual_start": "time",
            "actual_end": "time",
            "hours_worked": "float",
            "overtime_hours": "float",
            "attendance_status": "str",
            "remarks": "str"
        }
    },
    
    # ========== FINANCE MODULE ==========
    "fact_financial_transactions": {
        "required": {
            "transaction_id": "str",
            "transaction_date": "date",
            "transaction_type": "str",
            "account_code": "str",
            "description": "str",
            "debit_amount": "float",
            "credit_amount": "float",
            "balance": "float",
            "reference_number": "str",
            "vendor_customer_id": "str",
            "payment_method": "str",
            "status": "str"
        }
    },
    
    "dim_accounts": {
        "required": {
            "account_code": "str",
            "account_name": "str",
            "account_type": "str",
            "category": "str",
            "normal_balance": "str",
            "opening_balance": "float",
            "current_balance": "float",
            "parent_account": "str",
            "is_active": "bool"
        }
    },
    
    # ========== E-COMMERCE MODULE ==========
    "fact_ecommerce_orders": {
        "required": {
            "order_id": "str",
            "order_date": "datetime",
            "customer_id": "str",
            "total_amount": "float",
            "shipping_amount": "float",
            "tax_amount": "float",
            "discount_amount": "float",
            "net_amount": "float",
            "payment_status": "str",
            "order_status": "str",
            "shipping_method": "str",
            "tracking_number": "str",
            "website_session_id": "str"
        }
    },
    
    "fact_website_traffic": {
        "required": {
            "session_id": "str",
            "timestamp": "datetime",
            "page_url": "str",
            "referrer": "str",
            "device_type": "str",
            "location": "str",
            "session_duration_seconds": "int",
            "page_views": "int",
            "converted": "bool",
            "customer_id": "str"
        }
    },
    
    # ========== ANALYTICS MODULE ==========
    "fact_production_analytics": {
        "required": {
            "analytics_id": "str",
            "date": "date",
            "batch_id": "str",
            "equipment_id": "str",
            "oee_percentage": "float",
            "yield_rate": "float",
            "cycle_time_hours": "float",
            "downtime_hours": "float",
            "quality_score": "float",
            "cost_variance": "float",
            "energy_consumption": "float",
            "scrap_percentage": "float"
        }
    },
    
    "fact_inventory_analytics": {
        "required": {
            "analytics_id": "str",
            "date": "date",
            "sku": "str",
            "category": "str",
            "stock_turnover_rate": "float",
            "days_inventory_outstanding": "float",
            "stockout_frequency": "float",
            "holding_cost_percentage": "float",
            "abc_classification": "str",
            "excess_stock_value": "float",
            "stockout_value": "float",
            "forecast_accuracy": "float"
        }
    },
    
    # ========== SYSTEM MODULE ==========
    "dim_tenants": {
        "required": {
            "tenant_id": "str",
            "tenant_name": "str",
            "business_type": "str",
            "industry": "str",
            "subscription_plan": "str",
            "subscription_start": "date",
            "subscription_end": "date",
            "max_users": "int",
            "active_users": "int",
            "monthly_fee": "float",
            "status": "str",
            "contact_email": "str",
            "contact_phone": "str"
        }
    },
    
    "fact_audit_logs": {
        "required": {
            "log_id": "str",
            "timestamp": "datetime",
            "tenant_id": "str",
            "user_id": "str",
            "user_role": "str",
            "module": "str",
            "action": "str",
            "resource": "str",
            "resource_id": "str",
            "ip_address": "str",
            "success": "bool",
            "details": "str"
        }
    }
}

# ==================== DATA GENERATION FUNCTIONS ====================

def generate_tenants_data():
    """Generate tenant data for Cinta Beauty and demo tenants"""
    tenants = [
        {
            "tenant_id": "cinta_beauty",
            "tenant_name": "Cinta Beauty Ltd",
            "business_type": "Manufacturing",
            "industry": "Cosmetics & Beauty Products",
            "subscription_plan": "Enterprise",
            "subscription_start": date(2023, 1, 1),
            "subscription_end": date(2025, 12, 31),
            "max_users": 50,
            "active_users": 35,
            "monthly_fee": 250000.00,
            "status": "Active",
            "contact_email": "admin@cintabeauty.co.ke",
            "contact_phone": "+254 700 123 456"
        },
        {
            "tenant_id": "virtual_analytics",
            "tenant_name": "Virtual Analytics Demo",
            "business_type": "Consulting",
            "industry": "Technology Services",
            "subscription_plan": "Growth",
            "subscription_start": date(2024, 6, 1),
            "subscription_end": date(2024, 12, 31),
            "max_users": 20,
            "active_users": 15,
            "monthly_fee": 150000.00,
            "status": "Active",
            "contact_email": "demo@virtualanalytics.com",
            "contact_phone": "+254 711 987 654"
        },
        {
            "tenant_id": "sme_demo_01",
            "tenant_name": "SME Manufacturing Demo",
            "business_type": "Manufacturing",
            "industry": "FMCG",
            "subscription_plan": "Starter",
            "subscription_start": date(2024, 3, 15),
            "subscription_end": date(2024, 9, 14),
            "max_users": 10,
            "active_users": 8,
            "monthly_fee": 75000.00,
            "status": "Active",
            "contact_email": "info@smedemo.co.ke",
            "contact_phone": "+254 722 456 789"
        }
    ]
    return pd.DataFrame(tenants)

def generate_formulations_data():
    """Generate Cinta Beauty product formulations"""
    formulations = []
    product_categories = {
        "Skincare": ["Vitamin C Serum", "Hyaluronic Acid Serum", "Retinol Night Cream", "SPF 50 Sunscreen", 
                    "Tea Tree Face Wash", "Niacinamide Toner", "Ceramide Moisturizer", "AHA/BHA Exfoliant"],
        "Haircare": ["Argan Oil Shampoo", "Biotin Conditioner", "Hair Growth Serum", "Scalp Treatment", 
                    "Leave-in Conditioner", "Hair Mask", "Heat Protectant", "Color Protection Serum"],
        "Bodycare": ["Body Lotion", "Body Butter", "Body Scrub", "Body Oil", "Hand Cream", "Foot Cream", 
                    "Deodorant", "Body Mist"],
        "Makeup": ["Foundation", "Concealer", "Lipstick", "Mascara", "Blush", "Highlighter", "Eyeshadow", "Setting Spray"]
    }
    
    formulation_id = 1
    for category, products in product_categories.items():
        for product in products:
            formulations.append({
                "formulation_id": f"FMT-{formulation_id:04d}",
                "product_name": product,
                "product_category": category,
                "version": f"v{random.randint(1, 3)}.{random.randint(0, 9)}",
                "status": random.choice(["Active", "Active", "Active", "Archived"]),
                "created_date": date(2023, random.randint(1, 12), random.randint(1, 28)),
                "expected_yield_percentage": round(random.uniform(85, 98), 2),
                "production_cost_per_unit": round(random.uniform(150, 5000), 2),
                "target_cycle_time_hours": round(random.uniform(2, 48), 1),
                "quality_standards": random.choice(["KEBS", "FDA", "ISO 22716", "EU Regulations"])
            })
            formulation_id += 1
    
    return pd.DataFrame(formulations)

def generate_raw_materials_data():
    """Generate raw materials for cosmetic manufacturing"""
    materials = []
    categories = {
        "Active Ingredients": ["Vitamin C (Ascorbic Acid)", "Hyaluronic Acid", "Retinol", "Niacinamide", 
                              "Salicylic Acid", "Glycolic Acid", "Ceramides", "Peptides", "Coenzyme Q10"],
        "Base Oils": ["Jojoba Oil", "Argan Oil", "Rosehip Oil", "Coconut Oil", "Almond Oil", 
                     "Olive Oil", "Grapeseed Oil", "Sunflower Oil", "Shea Butter"],
        "Emulsifiers": ["Cetearyl Alcohol", "Glyceryl Stearate", "Polysorbate 80", "Lecithin", 
                       "Cetyl Alcohol", "Stearic Acid"],
        "Preservatives": ["Phenoxyethanol", "Potassium Sorbate", "Sodium Benzoate", "Benzyl Alcohol"],
        "Fragrances": ["Rose Essential Oil", "Lavender Oil", "Tea Tree Oil", "Peppermint Oil", 
                      "Citrus Blend", "Unscented"],
        "Packaging": ["Glass Bottle 30ml", "Plastic Tube 50ml", "Airless Pump", "Dropper", 
                     "Jar 60ml", "Spray Bottle", "Box Packaging", "Labels"]
    }
    
    suppliers = [f"SUP-{i:03d}" for i in range(1, 21)]
    
    material_id = 1
    for category, items in categories.items():
        for item in items:
            unit_cost = random.uniform(50, 5000)
            materials.append({
                "material_id": f"MAT-{material_id:04d}",
                "material_name": item,
                "category": category,
                "supplier_id": random.choice(suppliers),
                "unit_of_measure": random.choice(["kg", "liters", "pieces", "grams"]),
                "unit_cost": round(unit_cost, 2),
                "min_stock_level": random.randint(10, 100),
                "current_stock": random.randint(50, 500),
                "shelf_life_days": random.choice([180, 365, 730, 1095]),
                "storage_requirements": random.choice(["Room Temp", "Cool Dry Place", "Refrigerated", "Air Tight"])
            })
            material_id += 1
    
    return pd.DataFrame(materials)

def generate_employees_data():
    """Generate employee data for manufacturing company"""
    employees = []
    departments = {
        "Production": ["Production Manager", "Line Supervisor", "Machine Operator", "Quality Technician", 
                      "Production Worker", "Maintenance Technician"],
        "Quality Control": ["QC Manager", "Lab Technician", "Quality Inspector", "Compliance Officer"],
        "Research & Development": ["R&D Manager", "Formulation Scientist", "Product Developer", "Lab Assistant"],
        "Sales & Marketing": ["Sales Manager", "Marketing Executive", "Account Manager", "Customer Service"],
        "Finance & Admin": ["Finance Manager", "Accountant", "HR Manager", "Admin Assistant"],
        "Supply Chain": ["Procurement Manager", "Inventory Controller", "Logistics Coordinator", "Warehouse Supervisor"]
    }
    
    first_names = ["John", "Mary", "James", "Elizabeth", "Robert", "Sarah", "Michael", "Susan", 
                   "David", "Margaret", "William", "Dorothy", "Richard", "Lisa", "Charles", "Nancy",
                   "Joseph", "Karen", "Thomas", "Betty", "Daniel", "Helen", "Paul", "Sandra",
                   "Mark", "Ashley", "Donald", "Kimberly", "George", "Emily", "Kenneth", "Donna"]
    
    last_names = ["Smith", "Johnson", "Williams", "Jones", "Brown", "Davis", "Miller", "Wilson",
                  "Moore", "Taylor", "Anderson", "Thomas", "Jackson", "White", "Harris", "Martin",
                  "Thompson", "Garcia", "Martinez", "Robinson", "Clark", "Rodriguez", "Lewis", "Lee"]
    
    employee_id = 1
    for dept, positions in departments.items():
        for position in positions:
            first_name = random.choice(first_names)
            last_name = random.choice(last_names)
            email = f"{first_name.lower()}.{last_name.lower()}@cintabeauty.co.ke"
            hire_date = date(2020 + random.randint(0, 4), random.randint(1, 12), random.randint(1, 28))
            
            # Salary ranges by position
            if "Manager" in position:
                salary = random.uniform(150000, 300000)
            elif "Senior" in position or "Lead" in position:
                salary = random.uniform(80000, 150000)
            else:
                salary = random.uniform(30000, 80000)
            
            employees.append({
                "employee_id": f"EMP-{employee_id:04d}",
                "first_name": first_name,
                "last_name": last_name,
                "email": email,
                "phone": f"+254 7{random.randint(10, 99)} {random.randint(100000, 999999)}",
                "department": dept,
                "position": position,
                "hire_date": hire_date,
                "salary": round(salary, 2),
                "employment_type": random.choice(["Permanent", "Contract", "Temporary"]),
                "supervisor_id": f"EMP-{random.randint(1, 5):04d}" if employee_id > 5 else "",
                "shift_pattern": random.choice(["Morning", "Evening", "Night", "Flexible"]),
                "active": random.choice([True, True, True, False])
            })
            employee_id += 1
    
    return pd.DataFrame(employees)

def generate_customers_data():
    """Generate customer data for B2B and B2C"""
    customers = []
    cities = ["Nairobi", "Mombasa", "Kisumu", "Nakuru", "Eldoret", "Thika", "Malindi", "Kitale"]
    
    # B2B Customers (Retailers, Distributors)
    b2b_names = ["Nairobi Beauty Mart", "Coast Cosmetics Distributors", "Lake Region Retail", "Rift Valley Supplies",
                 "Mombasa Mall Stores", "Kisumu City Suppliers", "Nakuru Wholesalers", "Eldoret Trading Co",
                 "Thika Retail Hub", "Malindi Beach Stores", "Kitale Northern Distributors", "Central Kenya Suppliers"]
    
    # B2C Customer names
    first_names = ["Grace", "Faith", "Joy", "Hope", "Mercy", "Blessing", "Victor", "Samuel", "Peter", "Joseph",
                   "Ruth", "Esther", "Naomi", "Hannah", "David", "Isaac", "Jacob", "Daniel", "Stephen", "Andrew"]
    last_names = ["Mwangi", "Kariuki", "Ochieng", "Odhiambo", "Kamau", "Wanjiru", "Akinyi", "Achieng", "Otieno", "Omondi"]
    
    customer_id = 1
    
    # B2B Customers
    for name in b2b_names:
        city = random.choice(cities)
        customers.append({
            "customer_id": f"B2B-{customer_id:04d}",
            "customer_name": name,
            "customer_type": "Business",
            "email": f"orders@{name.lower().replace(' ', '')}.co.ke",
            "phone": f"+254 7{random.randint(10, 99)} {random.randint(100000, 999999)}",
            "address": f"{random.randint(1, 999)} {random.choice(['Moi', 'Kenyatta', 'Uhuru', 'Koinange'])} Avenue",
            "city": city,
            "country": "Kenya",
            "customer_since": date(2020 + random.randint(0, 4), random.randint(1, 12), random.randint(1, 28)),
            "loyalty_points": random.randint(1000, 10000),
            "total_purchases": round(random.uniform(50000, 500000), 2),
            "last_purchase_date": date(2024, random.randint(1, 12), random.randint(1, 28))
        })
        customer_id += 1
    
    # B2C Customers
    for i in range(50):
        first = random.choice(first_names)
        last = random.choice(last_names)
        city = random.choice(cities)
        customers.append({
            "customer_id": f"B2C-{customer_id:04d}",
            "customer_name": f"{first} {last}",
            "customer_type": "Individual",
            "email": f"{first.lower()}.{last.lower()}@gmail.com",
            "phone": f"+254 7{random.randint(10, 99)} {random.randint(100000, 999999)}",
            "address": f"{random.randint(1, 999)} {random.choice(['Street', 'Road', 'Avenue', 'Drive'])}",
            "city": city,
            "country": "Kenya",
            "customer_since": date(2022 + random.randint(0, 2), random.randint(1, 12), random.randint(1, 28)),
            "loyalty_points": random.randint(0, 500),
            "total_purchases": round(random.uniform(1000, 50000), 2),
            "last_purchase_date": date(2024, random.randint(1, 12), random.randint(1, 28))
        })
        customer_id += 1
    
    return pd.DataFrame(customers)

def generate_production_batches(formulations_df, employees_df):
    """Generate production batch data with manufacturing flow"""
    batches = []
    
    formulations = formulations_df[formulations_df['status'] == 'Active'].to_dict('records')
    production_staff = employees_df[employees_df['department'] == 'Production']['employee_id'].tolist()
    
    batch_id = 1
    for _ in range(100):  # Generate 100 batches
        formulation = random.choice(formulations)
        production_date = date(2024, random.randint(1, 12), random.randint(1, 28))
        
        planned_qty = random.randint(100, 1000)
        actual_qty = int(planned_qty * (formulation['expected_yield_percentage'] / 100) * random.uniform(0.9, 1.1))
        yield_pct = (actual_qty / planned_qty) * 100
        
        status = random.choice(['Completed', 'Completed', 'Completed', 'In Progress', 'Quality Hold'])
        
        # Production cost calculation
        unit_cost = formulation['production_cost_per_unit']
        total_cost = actual_qty * unit_cost * random.uniform(0.95, 1.05)
        
        # Quality score based on yield and variance
        quality_score = min(100, yield_pct * random.uniform(0.8, 1.2))
        
        batches.append({
            "batch_id": f"BATCH-{batch_id:04d}",
            "formulation_id": formulation['formulation_id'],
            "production_date": production_date,
            "planned_quantity": planned_qty,
            "actual_quantity": actual_qty,
            "yield_percentage": round(yield_pct, 2),
            "status": status,
            "equipment_id": f"EQP-{random.randint(1, 10):03d}",
            "supervisor_id": random.choice(production_staff),
            "quality_score": round(quality_score, 1),
            "total_cost": round(total_cost, 2),
            "completion_time_hours": round(formulation['target_cycle_time_hours'] * random.uniform(0.8, 1.3), 1)
        })
        batch_id += 1
    
    return pd.DataFrame(batches)

def generate_inventory_items(batches_df, formulations_df):
    """Generate inventory items from production batches"""
    inventory_items = []
    
    # Create a mapping from formulation_id to product_name
    formulation_to_product = dict(zip(formulations_df['formulation_id'], formulations_df['product_name']))
    formulation_to_category = dict(zip(formulations_df['formulation_id'], formulations_df['product_category']))
    
    # Create inventory items from formulations
    for formulation_id, product_name in formulation_to_product.items():
        category = formulation_to_category[formulation_id]
        
        # Generate SKU from product name
        sku_base = ''.join([word[:3].upper() for word in product_name.split()])
        sku = f"{sku_base}-{category[:3].upper()}-{random.randint(100, 999)}"
        
        # Get batches for this formulation
        formulation_batches = batches_df[batches_df['formulation_id'] == formulation_id]
        total_produced = formulation_batches['actual_quantity'].sum() if not formulation_batches.empty else 0
        
        # Get cost from formulations_df
        formulation_row = formulations_df[formulations_df['formulation_id'] == formulation_id].iloc[0]
        unit_cost = formulation_row['production_cost_per_unit']
        
        # Simulate sales to determine current stock
        current_stock = int(total_produced * random.uniform(0.1, 0.7))
        
        inventory_items.append({
            "sku": sku,
            "item_name": product_name,
            "category": category,
            "unit_of_measure": random.choice(["pcs", "ml", "g", "jar"]),
            "current_stock": current_stock,
            "min_stock_level": random.randint(10, 100),
            "max_stock_level": random.randint(500, 5000),
            "average_unit_cost": unit_cost,
            "total_value": round(current_stock * unit_cost, 2),
            "last_restock_date": date(2024, random.randint(1, 12), random.randint(1, 28)),
            "expiry_date": date(2025 + random.randint(0, 2), random.randint(1, 12), random.randint(1, 28)),
            "storage_location": random.choice(["Warehouse A", "Warehouse B", "Cold Room", "Shelf Storage"])
        })
    
    return pd.DataFrame(inventory_items)

def generate_inventory_transactions(inventory_items_df, batches_df, employees_df, formulations_df):
    """Generate inventory transactions (receiving, issuing, transfers)"""
    transactions = []
    
    inventory_staff = employees_df[employees_df['department'].isin(['Supply Chain', 'Production'])]['employee_id'].tolist()
    transaction_id = 1
    
    # Create a mapping from formulation_id to product_name
    formulation_to_product = dict(zip(formulations_df['formulation_id'], formulations_df['product_name']))
    
    # Production Receipts
    for _, batch in batches_df.iterrows():
        if batch['status'] == 'Completed':
            # Find inventory item for this batch
            formulation_id = batch['formulation_id']
            product_name = formulation_to_product.get(formulation_id, "")
            
            if product_name:
                # Find inventory item by product name
                inventory_item = inventory_items_df[inventory_items_df['item_name'] == product_name]
                
                if not inventory_item.empty:
                    sku = inventory_item.iloc[0]['sku']
                    unit_cost = inventory_item.iloc[0]['average_unit_cost']
                    
                    transactions.append({
                        "transaction_id": f"INV-{transaction_id:06d}",
                        "transaction_date": batch['production_date'],
                        "transaction_type": "Production Receipt",
                        "sku": sku,
                        "batch_id": batch['batch_id'],
                        "quantity": batch['actual_quantity'],
                        "unit_cost": unit_cost,
                        "total_value": round(batch['actual_quantity'] * unit_cost, 2),
                        "location_id": "WH01",
                        "reference_document": f"PROD-{batch['batch_id']}",
                        "employee_id": random.choice(inventory_staff)
                    })
                    transaction_id += 1
    
    # Sales Issues
    for _, item in inventory_items_df.iterrows():
        if item['current_stock'] > 0:
            # Generate some sales transactions
            sales_qty = random.randint(1, min(100, item['current_stock']))
            for _ in range(random.randint(1, 5)):
                transactions.append({
                    "transaction_id": f"INV-{transaction_id:06d}",
                    "transaction_date": date(2024, random.randint(1, 12), random.randint(1, 28)),
                    "transaction_type": "Sales Issue",
                    "sku": item['sku'],
                    "batch_id": f"BATCH-{random.randint(1, 100):04d}",
                    "quantity": -sales_qty,  # Negative for issues
                    "unit_cost": item['average_unit_cost'],
                    "total_value": round(-sales_qty * item['average_unit_cost'], 2),
                    "location_id": "WH01",
                    "reference_document": f"SALE-{random.randint(1000, 9999)}",
                    "employee_id": random.choice(inventory_staff)
                })
                transaction_id += 1
    
    # Stock Transfers
    for _ in range(20):
        item = random.choice(inventory_items_df.to_dict('records'))
        transactions.append({
            "transaction_id": f"INV-{transaction_id:06d}",
            "transaction_date": date(2024, random.randint(1, 12), random.randint(1, 28)),
            "transaction_type": "Stock Transfer",
            "sku": item['sku'],
            "batch_id": f"BATCH-{random.randint(1, 100):04d}",
            "quantity": random.randint(10, 100),
            "unit_cost": item['average_unit_cost'],
            "total_value": round(random.randint(10, 100) * item['average_unit_cost'], 2),
            "location_id": random.choice(["WH01", "WH02", "STORE01"]),
            "reference_document": f"TRF-{random.randint(1000, 9999)}",
            "employee_id": random.choice(inventory_staff)
        })
        transaction_id += 1
    
    return pd.DataFrame(transactions)

def generate_sales_transactions(inventory_items_df, customers_df, employees_df):
    """Generate sales transactions"""
    sales = []
    
    sales_staff = employees_df[employees_df['department'] == 'Sales & Marketing']['employee_id'].tolist()
    customers = customers_df.to_dict('records')
    inventory_items = inventory_items_df.to_dict('records')
    
    transaction_id = 1
    for _ in range(500):  # Generate 500 sales transactions
        customer = random.choice(customers)
        item = random.choice(inventory_items)
        
        # Determine unit price (markup from cost)
        markup = random.uniform(1.5, 3.0)  # 50-200% markup
        unit_price = round(item['average_unit_cost'] * markup, 2)
        
        quantity = random.randint(1, 20)
        total_amount = round(unit_price * quantity, 2)
        
        # Apply discounts for B2B customers
        if customer['customer_type'] == 'Business':
            discount_pct = random.uniform(0.05, 0.20)
        else:
            discount_pct = random.uniform(0, 0.10)
        
        discount_amount = round(total_amount * discount_pct, 2)
        tax_amount = round((total_amount - discount_amount) * 0.16, 2)  # 16% VAT in Kenya
        net_amount = round(total_amount - discount_amount + tax_amount, 2)
        
        sales_date = datetime(2024, random.randint(1, 12), random.randint(1, 28), 
                            random.randint(8, 20), random.randint(0, 59))
        
        sales.append({
            "transaction_id": f"SALE-{transaction_id:06d}",
            "transaction_date": sales_date,
            "customer_id": customer['customer_id'],
            "sku": item['sku'],
            "quantity": quantity,
            "unit_price": unit_price,
            "total_amount": total_amount,
            "discount_amount": discount_amount,
            "tax_amount": tax_amount,
            "net_amount": net_amount,
            "payment_method": random.choice(["M-Pesa", "Cash", "Bank Transfer", "Credit Card"]),
            "payment_status": random.choice(["Paid", "Paid", "Paid", "Pending", "Partial"]),
            "sales_channel": random.choice(["Retail Store", "Online", "Wholesale", "Distributor"]),
            "employee_id": random.choice(sales_staff)
        })
        transaction_id += 1
    
    return pd.DataFrame(sales)

def generate_ecommerce_orders(customers_df, sales_df):
    """Generate e-commerce orders"""
    orders = []
    
    # Filter for individual customers
    b2c_customers = customers_df[customers_df['customer_type'] == 'Individual'].to_dict('records')
    
    order_id = 1
    for _ in range(200):  # 200 online orders
        customer = random.choice(b2c_customers)
        
        # Generate order date
        order_date = datetime(2024, random.randint(1, 12), random.randint(1, 28),
                            random.randint(0, 23), random.randint(0, 59))
        
        # Generate order amounts
        subtotal = round(random.uniform(1000, 50000), 2)
        shipping = round(random.uniform(200, 2000), 2)
        tax = round(subtotal * 0.16, 2)
        discount = round(subtotal * random.uniform(0, 0.15), 2)
        net_amount = round(subtotal + shipping + tax - discount, 2)
        
        orders.append({
            "order_id": f"ECOMM-{order_id:06d}",
            "order_date": order_date,
            "customer_id": customer['customer_id'],
            "total_amount": subtotal,
            "shipping_amount": shipping,
            "tax_amount": tax,
            "discount_amount": discount,
            "net_amount": net_amount,
            "payment_status": random.choice(["Paid", "Paid", "Paid", "Pending", "Failed"]),
            "order_status": random.choice(["Delivered", "Shipped", "Processing", "Cancelled"]),
            "shipping_method": random.choice(["Standard", "Express", "Pickup", "Courier"]),
            "tracking_number": f"TRACK-{random.randint(1000000000, 9999999999)}",
            "website_session_id": f"SESS-{random.randint(100000, 999999)}"
        })
        order_id += 1
    
    return pd.DataFrame(orders)

def generate_attendance_data(employees_df):
    """Generate attendance data for employees"""
    attendance = []
    
    active_employees = employees_df[employees_df['active'] == True].to_dict('records')
    attendance_id = 1
    
    # Generate 30 days of attendance data
    for day_offset in range(30):
        current_date = date(2024, 6, 1) + timedelta(days=day_offset)
        
        for employee in active_employees:
            # Skip weekends with 90% probability
            if current_date.weekday() >= 5 and random.random() > 0.1:
                continue
            
            # Determine shift based on pattern
            if employee['shift_pattern'] == 'Morning':
                shift_start = datetime.strptime('08:00', '%H:%M').time()
                shift_end = datetime.strptime('17:00', '%H:%M').time()
            elif employee['shift_pattern'] == 'Evening':
                shift_start = datetime.strptime('14:00', '%H:%M').time()
                shift_end = datetime.strptime('23:00', '%H:%M').time()
            else:
                shift_start = datetime.strptime('08:00', '%H:%M').time()
                shift_end = datetime.strptime('17:00', '%H:%M').time()
            
            # Simulate actual times with some variance
            late_minutes = random.randint(0, 30) if random.random() > 0.8 else 0
            early_leave = random.randint(0, 60) if random.random() > 0.9 else 0
            
            actual_start = (datetime.combine(current_date, shift_start) + 
                          timedelta(minutes=late_minutes)).time()
            actual_end = (datetime.combine(current_date, shift_end) - 
                         timedelta(minutes=early_leave)).time()
            
            hours_worked = ((datetime.combine(current_date, actual_end) - 
                           datetime.combine(current_date, actual_start)).seconds / 3600)
            
            overtime = max(0, hours_worked - 9)  # Overtime after 9 hours
            
            attendance.append({
                "attendance_id": f"ATT-{attendance_id:06d}",
                "employee_id": employee['employee_id'],
                "date": current_date,
                "shift_start": shift_start,
                "shift_end": shift_end,
                "actual_start": actual_start,
                "actual_end": actual_end,
                "hours_worked": round(hours_worked, 2),
                "overtime_hours": round(overtime, 2),
                "attendance_status": random.choice(["Present", "Present", "Present", "Late", "Half Day"]),
                "remarks": random.choice(["", "", "Traffic", "Family Emergency", "Medical Appointment"])
            })
            attendance_id += 1
    
    return pd.DataFrame(attendance)

def generate_financial_accounts():
    """Generate chart of accounts"""
    accounts = []
    
    account_structure = {
        "Assets": {
            "Current Assets": ["Cash", "Accounts Receivable", "Inventory", "Prepaid Expenses"],
            "Fixed Assets": ["Equipment", "Vehicles", "Buildings", "Furniture & Fixtures"]
        },
        "Liabilities": {
            "Current Liabilities": ["Accounts Payable", "Accrued Expenses", "Short-term Loans"],
            "Long-term Liabilities": ["Long-term Loans", "Mortgage Payable"]
        },
        "Equity": {
            "Owner's Equity": ["Capital", "Retained Earnings", "Drawings"]
        },
        "Revenue": {
            "Sales Revenue": ["Product Sales", "Service Revenue", "Online Sales"],
            "Other Revenue": ["Interest Income", "Discounts Received"]
        },
        "Expenses": {
            "Cost of Goods Sold": ["Raw Materials", "Direct Labor", "Manufacturing Overhead"],
            "Operating Expenses": ["Salaries", "Rent", "Utilities", "Marketing", "Office Supplies"],
            "Financial Expenses": ["Bank Charges", "Interest Expense"]
        }
    }
    
    account_code = 1000
    for main_category, subcategories in account_structure.items():
        for subcategory, account_names in subcategories.items():
            for account_name in account_names:
                # Determine normal balance
                if main_category in ["Assets", "Expenses"]:
                    normal_balance = "Debit"
                else:
                    normal_balance = "Credit"
                
                # Generate opening balance
                if main_category == "Assets":
                    opening_balance = random.uniform(100000, 1000000)
                elif main_category == "Liabilities":
                    opening_balance = random.uniform(50000, 500000)
                elif main_category == "Equity":
                    opening_balance = random.uniform(500000, 2000000)
                elif main_category == "Revenue":
                    opening_balance = random.uniform(0, 100000)
                else:  # Expenses
                    opening_balance = random.uniform(0, 500000)
                
                accounts.append({
                    "account_code": str(account_code),
                    "account_name": account_name,
                    "account_type": main_category,
                    "category": subcategory,
                    "normal_balance": normal_balance,
                    "opening_balance": round(opening_balance, 2),
                    "current_balance": round(opening_balance * random.uniform(0.8, 1.2), 2),
                    "parent_account": "",
                    "is_active": True
                })
                account_code += 1
    
    return pd.DataFrame(accounts)

def generate_financial_transactions(accounts_df, sales_df):
    """Generate financial transactions"""
    transactions = []
    
    accounts = accounts_df.to_dict('records')
    transaction_id = 1
    
    # Sales transactions
    for _, sale in sales_df.iterrows():
        if sale['payment_status'] == 'Paid':
            # Debit: Cash/Bank, Credit: Sales Revenue
            cash_account = next(acc for acc in accounts if "Cash" in acc['account_name'] or "Bank" in acc['account_name'])
            sales_account = next(acc for acc in accounts if "Sales" in acc['account_name'])
            
            transactions.append({
                "transaction_id": f"FIN-{transaction_id:06d}",
                "transaction_date": sale['transaction_date'].date(),
                "transaction_type": "Sales Receipt",
                "account_code": cash_account['account_code'],
                "description": f"Sale to {sale['customer_id']}",
                "debit_amount": sale['net_amount'],
                "credit_amount": 0,
                "balance": cash_account['current_balance'] + sale['net_amount'],
                "reference_number": sale['transaction_id'],
                "vendor_customer_id": sale['customer_id'],
                "payment_method": sale['payment_method'],
                "status": "Posted"
            })
            transaction_id += 1
            
            transactions.append({
                "transaction_id": f"FIN-{transaction_id:06d}",
                "transaction_date": sale['transaction_date'].date(),
                "transaction_type": "Sales Revenue",
                "account_code": sales_account['account_code'],
                "description": f"Sale to {sale['customer_id']}",
                "debit_amount": 0,
                "credit_amount": sale['net_amount'] - sale['tax_amount'],
                "balance": sales_account['current_balance'] + (sale['net_amount'] - sale['tax_amount']),
                "reference_number": sale['transaction_id'],
                "vendor_customer_id": sale['customer_id'],
                "payment_method": "",
                "status": "Posted"
            })
            transaction_id += 1
    
    # Expense transactions
    for _ in range(100):
        expense_date = date(2024, random.randint(1, 12), random.randint(1, 28))
        expense_account = random.choice([acc for acc in accounts if acc['account_type'] == 'Expenses'])
        amount = round(random.uniform(1000, 50000), 2)
        
        transactions.append({
            "transaction_id": f"FIN-{transaction_id:06d}",
            "transaction_date": expense_date,
            "transaction_type": "Expense Payment",
            "account_code": expense_account['account_code'],
            "description": random.choice(["Office Supplies", "Utility Bill", "Marketing Campaign", "Equipment Maintenance"]),
            "debit_amount": amount,
            "credit_amount": 0,
            "balance": expense_account['current_balance'] + amount,
            "reference_number": f"EXP-{random.randint(1000, 9999)}",
            "vendor_customer_id": f"VEND-{random.randint(1, 50):03d}",
            "payment_method": random.choice(["Bank Transfer", "M-Pesa", "Cheque"]),
            "status": "Posted"
        })
        transaction_id += 1
    
    return pd.DataFrame(transactions)

def generate_website_traffic():
    """Generate website traffic data"""
    traffic = []
    
    pages = [
        "/", "/products", "/products/skincare", "/products/haircare", 
        "/about", "/contact", "/cart", "/checkout", "/blog"
    ]
    
    devices = ["Desktop", "Mobile", "Tablet"]
    locations = ["Nairobi", "Mombasa", "Kisumu", "Nakuru", "Eldoret", "International"]
    
    session_id = 1
    for day_offset in range(30):
        current_date = date(2024, 6, 1) + timedelta(days=day_offset)
        
        # Generate sessions per day
        for _ in range(random.randint(50, 200)):
            session_start = datetime.combine(current_date, 
                                           datetime.strptime(f"{random.randint(8, 22)}:{random.randint(0, 59)}", "%H:%M").time())
            
            # Generate page views for this session
            page_views = random.randint(1, 10)
            session_duration = random.randint(30, 1800)  # 30 seconds to 30 minutes
            
            for view in range(page_views):
                view_time = session_start + timedelta(seconds=random.randint(0, session_duration))
                
                # 5% conversion rate
                converted = random.random() < 0.05
                customer_id = f"B2C-{random.randint(1000, 1050):04d}" if converted else ""
                
                traffic.append({
                    "session_id": f"SESS-{session_id:06d}",
                    "timestamp": view_time,
                    "page_url": random.choice(pages),
                    "referrer": random.choice(["Direct", "Google", "Facebook", "Instagram", "Email"]),
                    "device_type": random.choice(devices),
                    "location": random.choice(locations),
                    "session_duration_seconds": session_duration,
                    "page_views": page_views,
                    "converted": converted,
                    "customer_id": customer_id
                })
            
            session_id += 1
    
    return pd.DataFrame(traffic)

def generate_production_analytics(batches_df):
    """Generate production analytics data"""
    analytics = []
    
    analytics_id = 1
    for _, batch in batches_df.iterrows():
        if batch['status'] == 'Completed':
            oee = random.uniform(70, 95)  # Overall Equipment Effectiveness
            downtime = batch['completion_time_hours'] * random.uniform(0.05, 0.20)  # 5-20% downtime
            
            analytics.append({
                "analytics_id": f"PROD-ANAL-{analytics_id:04d}",
                "date": batch['production_date'],
                "batch_id": batch['batch_id'],
                "equipment_id": batch['equipment_id'],
                "oee_percentage": round(oee, 2),
                "yield_rate": round(batch['yield_percentage'], 2),
                "cycle_time_hours": round(batch['completion_time_hours'], 2),
                "downtime_hours": round(downtime, 2),
                "quality_score": round(batch['quality_score'], 1),
                "cost_variance": round(random.uniform(-0.1, 0.1) * batch['total_cost'], 2),
                "energy_consumption": round(batch['total_cost'] * random.uniform(0.05, 0.15), 2),
                "scrap_percentage": round(100 - batch['yield_percentage'], 2)
            })
            analytics_id += 1
    
    return pd.DataFrame(analytics)

def generate_inventory_analytics(inventory_items_df, inventory_transactions_df):
    """Generate inventory analytics data"""
    analytics = []
    
    analytics_id = 1
    for _, item in inventory_items_df.iterrows():
        # Calculate days for analysis
        for month in range(1, 13):
            analysis_date = date(2024, month, 15)
            
            # Get transactions for this item
            item_transactions = inventory_transactions_df[
                (inventory_transactions_df['sku'] == item['sku']) &
                (inventory_transactions_df['transaction_date'] <= analysis_date)
            ]
            
            if not item_transactions.empty:
                # Calculate metrics
                total_issues = abs(item_transactions[item_transactions['transaction_type'] == 'Sales Issue']['quantity'].sum())
                avg_inventory = item['current_stock']
                
                if avg_inventory > 0:
                    turnover_rate = total_issues / avg_inventory
                    days_inventory = 365 / turnover_rate if turnover_rate > 0 else 365
                else:
                    turnover_rate = 0
                    days_inventory = 0
                
                # ABC classification
                if item['total_value'] > 100000:
                    abc_class = 'A'
                elif item['total_value'] > 50000:
                    abc_class = 'B'
                else:
                    abc_class = 'C'
                
                analytics.append({
                    "analytics_id": f"INV-ANAL-{analytics_id:04d}",
                    "date": analysis_date,
                    "sku": item['sku'],
                    "category": item['category'],
                    "stock_turnover_rate": round(turnover_rate, 2),
                    "days_inventory_outstanding": round(days_inventory, 1),
                    "stockout_frequency": round(random.uniform(0, 0.2), 3),
                    "holding_cost_percentage": round(random.uniform(0.15, 0.35), 2),
                    "abc_classification": abc_class,
                    "excess_stock_value": round(item['total_value'] * random.uniform(0, 0.3), 2),
                    "stockout_value": round(item['total_value'] * random.uniform(0, 0.1), 2),
                    "forecast_accuracy": round(random.uniform(0.7, 0.95), 3)
                })
                analytics_id += 1
    
    return pd.DataFrame(analytics)

def generate_audit_logs(tenants_df, employees_df, sales_df):
    """Generate audit log data"""
    logs = []
    
    users = employees_df[['employee_id', 'position']].to_dict('records')
    
    log_id = 1
    for day_offset in range(90):  # 90 days of logs
        log_date = datetime(2024, 3, 1) + timedelta(days=day_offset)
        
        # Generate logs per day
        for _ in range(random.randint(10, 50)):
            user = random.choice(users)
            tenant = random.choice(tenants_df['tenant_id'].tolist())
            
            # Determine module and action based on user role
            if 'Manager' in user['position']:
                module = random.choice(['Production', 'Inventory', 'Sales', 'Finance', 'HR'])
            elif 'Sales' in user['position']:
                module = 'Sales'
            elif 'Production' in user['position']:
                module = 'Production'
            else:
                module = random.choice(['System', 'Inventory', 'HR'])
            
            actions = {
                'Production': ['CREATE_BATCH', 'UPDATE_FORMULATION', 'VIEW_REPORT', 'APPROVE_QC'],
                'Inventory': ['ADD_STOCK', 'ISSUE_STOCK', 'TRANSFER', 'ADJUST'],
                'Sales': ['CREATE_ORDER', 'PROCESS_PAYMENT', 'ISSUE_INVOICE', 'VIEW_CUSTOMER'],
                'Finance': ['POST_TRANSACTION', 'GENERATE_REPORT', 'APPROVE_PAYMENT', 'RECONCILE'],
                'HR': ['ADD_EMPLOYEE', 'UPDATE_SALARY', 'APPROVE_LEAVE', 'GENERATE_PAYROLL'],
                'System': ['LOGIN', 'LOGOUT', 'CHANGE_SETTING', 'VIEW_AUDIT']
            }
            
            action = random.choice(actions.get(module, ['VIEW', 'UPDATE']))
            
            # Generate IP address
            ip = f"192.168.{random.randint(1, 255)}.{random.randint(1, 255)}"
            
            logs.append({
                "log_id": f"AUDIT-{log_id:08d}",
                "timestamp": log_date.replace(hour=random.randint(8, 20), 
                                            minute=random.randint(0, 59),
                                            second=random.randint(0, 59)),
                "tenant_id": tenant,
                "user_id": user['employee_id'],
                "user_role": user['position'],
                "module": module,
                "action": action,
                "resource": random.choice(['Batch', 'Product', 'Customer', 'Order', 'Employee', 'Transaction']),
                "resource_id": random.choice([f"BATCH-{random.randint(1, 100):04d}", 
                                            f"PROD-{random.randint(1, 50):04d}",
                                            f"CUST-{random.randint(1, 100):04d}"]),
                "ip_address": ip,
                "success": random.choice([True, True, True, False]),
                "details": f"{action} operation on {module} module"
            })
            log_id += 1
    
    return pd.DataFrame(logs)

def validate_table(table_name: str, df: pd.DataFrame) -> tuple:
    """Validate generated data against schema"""
    if table_name not in SCHEMAS:
        return df, {"errors": ["Unknown table"], "warnings": [], "ok": False}
    
    schema = SCHEMAS[table_name]
    errors = []
    warnings = []
    
    # Check required columns
    required_cols = set(schema["required"].keys())
    actual_cols = set(df.columns)
    
    missing_cols = required_cols - actual_cols
    extra_cols = actual_cols - required_cols
    
    if missing_cols:
        errors.append(f"Missing columns: {missing_cols}")
    
    if extra_cols:
        warnings.append(f"Extra columns: {extra_cols}")
    
    # Check data types
    for col, expected_type in schema["required"].items():
        if col in df.columns:
            try:
                if expected_type == "date":
                    df[col] = pd.to_datetime(df[col]).dt.date
                elif expected_type == "datetime":
                    df[col] = pd.to_datetime(df[col])
                elif expected_type == "bool":
                    df[col] = df[col].astype(bool)
                elif expected_type == "int":
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
                elif expected_type == "float":
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0).astype(float)
                else:  # str
                    df[col] = df[col].astype(str)
            except Exception as e:
                errors.append(f"Type conversion failed for {col}: {str(e)}")
    
    # Check for nulls in required columns
    for col in required_cols:
        if col in df.columns and df[col].isnull().any():
            null_count = df[col].isnull().sum()
            warnings.append(f"Column {col} has {null_count} null values")
    
    return df, {
        "row_count": len(df),
        "errors": errors,
        "warnings": warnings,
        "ok": len(errors) == 0
    }

def summarize_dq(report: dict) -> pd.DataFrame:
    """Create DQ summary table"""
    summary = {
        "Metric": ["Total Rows", "Errors", "Warnings", "Validation Status"],
        "Value": [
            report["row_count"],
            len(report["errors"]),
            len(report["warnings"]),
            "✅ PASS" if report["ok"] else "❌ FAIL"
        ]
    }
    return pd.DataFrame(summary)

def generate_demo_data(seed: int = 42):
    """Main function to generate all demo data"""
    # Update random seed
    np.random.seed(seed)
    random.seed(seed)
    
    try:
        # Generate all datasets with proper error handling
        print("Step 1/17: Generating tenant data...")
        tenants_df = generate_tenants_data()
        
        print("Step 2/17: Generating employee data...")
        employees_df = generate_employees_data()
        
        print("Step 3/17: Generating product formulations...")
        formulations_df = generate_formulations_data()
        
        print("Step 4/17: Generating raw materials...")
        raw_materials_df = generate_raw_materials_data()
        
        print("Step 5/17: Generating customer data...")
        customers_df = generate_customers_data()
        
        print("Step 6/17: Generating production batches...")
        batches_df = generate_production_batches(formulations_df, employees_df)
        
        print("Step 7/17: Generating inventory items...")
        inventory_items_df = generate_inventory_items(batches_df, formulations_df)
        
        print("Step 8/17: Generating inventory transactions...")
        inventory_transactions_df = generate_inventory_transactions(
            inventory_items_df, batches_df, employees_df, formulations_df
        )
        
        print("Step 9/17: Generating sales transactions...")
        sales_df = generate_sales_transactions(inventory_items_df, customers_df, employees_df)
        
        print("Step 10/17: Generating e-commerce orders...")
        ecommerce_orders_df = generate_ecommerce_orders(customers_df, sales_df)
        
        print("Step 11/17: Generating attendance data...")
        attendance_df = generate_attendance_data(employees_df)
        
        print("Step 12/17: Generating financial accounts...")
        accounts_df = generate_financial_accounts()
        
        print("Step 13/17: Generating financial transactions...")
        financial_transactions_df = generate_financial_transactions(accounts_df, sales_df)
        
        print("Step 14/17: Generating website traffic...")
        website_traffic_df = generate_website_traffic()
        
        print("Step 15/17: Generating production analytics...")
        production_analytics_df = generate_production_analytics(batches_df)
        
        print("Step 16/17: Generating inventory analytics...")
        inventory_analytics_df = generate_inventory_analytics(inventory_items_df, inventory_transactions_df)
        
        print("Step 17/17: Generating audit logs...")
        audit_logs_df = generate_audit_logs(tenants_df, employees_df, sales_df)
        
        # Package all data
        datasets = {
            "dim_tenants": tenants_df,
            "dim_employees": employees_df,
            "dim_formulations": formulations_df,
            "dim_raw_materials": raw_materials_df,
            "dim_customers": customers_df,
            "fact_production_batches": batches_df,
            "dim_inventory_items": inventory_items_df,
            "fact_inventory_transactions": inventory_transactions_df,
            "fact_sales_transactions": sales_df,
            "fact_ecommerce_orders": ecommerce_orders_df,
            "fact_attendance": attendance_df,
            "dim_accounts": accounts_df,
            "fact_financial_transactions": financial_transactions_df,
            "fact_website_traffic": website_traffic_df,
            "fact_production_analytics": production_analytics_df,
            "fact_inventory_analytics": inventory_analytics_df,
            "fact_audit_logs": audit_logs_df
        }
        
        # Validate all datasets
        reports = {}
        for table_name, df in datasets.items():
            clean_df, report = validate_table(table_name, df)
            datasets[table_name] = clean_df
            reports[table_name] = {
                "row_count": report["row_count"],
                "errors": report["errors"],
                "warnings": report["warnings"],
                "ok": report["ok"]
            }
        
        return datasets, reports
    
    except Exception as e:
        raise Exception(f"Error generating demo data: {str(e)}")

# ==================== STREAMLIT APP ====================

def main():
    st.title("🛠️ Admin & Data | The Verse Manufacturing ERP")
    st.caption("Upload datasets, validate schemas, manage assumptions, and generate demo data for Cinta Beauty Manufacturing System.")
    
    tab1, tab2, tab3 = st.tabs(["📥 Data Ingestion", "⚙️ Assumptions", "📦 Export & Reset"])
    
    # Initialize session state for storing data
    if 'generated_data' not in st.session_state:
        st.session_state.generated_data = {}
    
    with tab1:
        st.subheader("Option A — Generate Demo Data (recommended first)")
        colA, colB = st.columns([1, 1])
        
        with colA:
            seed = st.number_input("Random seed", min_value=0, value=42, step=1, key="seed_input")
        
        with colB:
            if st.button("🚀 Generate Demo Data", type="primary", use_container_width=True):
                try:
                    with st.spinner("Generating comprehensive manufacturing data..."):
                        # Create progress bar
                        progress_bar = st.progress(0)
                        status_text = st.empty()
                        
                        # Update progress
                        status_text.text("Starting data generation...")
                        progress_bar.progress(5)
                        
                        # Generate data
                        datasets, reports = generate_demo_data(int(seed))
                        
                        # Store in session state
                        st.session_state.generated_data = datasets
                        
                        # Update progress
                        progress_bar.progress(100)
                        status_text.text("Data generation complete!")
                        
                        st.success(f"✅ Demo data generated successfully! {len(datasets)} tables loaded.")
                        
                        with st.expander("📋 Validation Reports"):
                            for name, rep in reports.items():
                                st.markdown(f"### {name}")
                                st.json(rep)
                                
                except Exception as e:
                    st.error(f"Error generating data: {str(e)}")
                    if st.checkbox("Show detailed error"):
                        st.exception(e)
        
        st.markdown("---")
        st.subheader("Option B — Upload files (CSV/Excel/Parquet)")
        st.write("Select a target table and upload a file. The app will validate and store it in memory.")
        
        col1, col2 = st.columns([1, 2], gap="large")
        
        with col1:
            table_name = st.selectbox("Target table", options=list(SCHEMAS.keys()))
            st.caption("Expected columns:")
            st.code("\n".join(list(SCHEMAS[table_name]["required"].keys())), language="text")
        
        with col2:
            uploaded = st.file_uploader("Upload data file", type=["csv", "xlsx", "xls", "parquet"])
            if uploaded is not None:
                try:
                    if uploaded.name.lower().endswith(".csv"):
                        df = pd.read_csv(uploaded)
                    elif uploaded.name.lower().endswith((".xlsx", ".xls")):
                        df = pd.read_excel(uploaded)
                    else:
                        df = pd.read_parquet(uploaded)
                    
                    clean, report = validate_table(table_name, df)
                    
                    st.markdown("### Preview")
                    st.dataframe(clean.head(50), use_container_width=True)
                    
                    st.markdown("### Validation")
                    cA, cB = st.columns([1, 2])
                    with cA:
                        st.metric("Rows", report["row_count"])
                        st.metric("Errors", len(report["errors"]))
                        st.metric("Warnings", len(report["warnings"]))
                    with cB:
                        if report["errors"]:
                            st.error("Errors: " + " | ".join(report["errors"]))
                        if report["warnings"]:
                            st.warning("Warnings: " + " | ".join(report["warnings"]))
                    
                    st.markdown("### DQ Summary (key metrics)")
                    st.dataframe(summarize_dq(report), use_container_width=True, hide_index=True)
                    
                    if report["ok"]:
                        if st.button("💾 Save into platform", type="primary"):
                            st.session_state.generated_data[table_name] = clean
                            st.success(f"Saved {table_name} into session store.")
                    else:
                        st.info("Fix schema errors before saving.")
                        
                except Exception as e:
                    st.error("Upload failed. Please check file format.")
                    st.exception(e)
        
        st.markdown("---")
        st.subheader("Currently loaded tables")
        
        if not st.session_state.generated_data:
            st.info("No tables loaded yet. Generate demo data or upload your extracts above.")
        else:
            for name, df in st.session_state.generated_data.items():
                with st.expander(f"{name} — {len(df):,} rows"):
                    st.dataframe(df.head(200), use_container_width=True)
    
    with tab2:
        st.subheader("Scenario & Model Assumptions (JSON)")
        st.write("Edit assumptions used across manufacturing, inventory, and financial models.")
        
        left, right = st.columns([1, 1], gap="large")
        
        with left:
            st.markdown("**Manufacturing Assumptions**")
            
            col1, col2 = st.columns(2)
            with col1:
                default_yield = st.number_input(
                    "Default Yield %",
                    min_value=0.0,
                    max_value=100.0,
                    value=92.5,
                    step=0.5
                )
                vat_rate = st.number_input(
                    "VAT Rate %",
                    min_value=0.0,
                    max_value=50.0,
                    value=16.0,
                    step=0.5
                )
            
            with col2:
                markup_multiplier = st.number_input(
                    "Default Markup",
                    min_value=1.0,
                    max_value=5.0,
                    value=2.5,
                    step=0.1
                )
                holding_cost = st.number_input(
                    "Holding Cost %",
                    min_value=0.0,
                    max_value=50.0,
                    value=25.0,
                    step=0.5
                )
            
            if st.button("Apply Manufacturing Assumptions", type="primary"):
                st.success("Assumptions updated for this session.")
        
        with right:
            st.markdown("**Raw JSON Configuration** (advanced)")
            default_config = {
                "manufacturing": {
                    "default_yield_percentage": 92.5,
                    "target_oee": 85.0,
                    "standard_cycle_time_hours": 24.0,
                    "quality_threshold": 95.0
                },
                "inventory": {
                    "holding_cost_percentage": 25.0,
                    "service_level_target": 95.0,
                    "lead_time_days": 14,
                    "safety_stock_multiplier": 1.5
                },
                "finance": {
                    "vat_rate": 16.0,
                    "discount_rate": 10.0,
                    "inflation_rate": 6.0,
                    "currency": "KES"
                },
                "kenya_specific": {
                    "kra_compliant": True,
                    "kebs_standards": True,
                    "mpesa_integrated": True,
                    "local_suppliers": True
                }
            }
            
            text = st.text_area("Assumptions JSON", value=json.dumps(default_config, indent=2), height=300)
            
            if st.button("Load JSON Configuration", use_container_width=True):
                try:
                    config = json.loads(text)
                    st.success("Configuration loaded successfully!")
                    st.json(config)
                except Exception as e:
                    st.error("Invalid JSON format")
    
    with tab3:
        st.subheader("Export & Reset")
        st.write("Export loaded tables or reset the session store.")
        
        if st.session_state.generated_data:
            export_name = st.selectbox("Select table to export", options=list(st.session_state.generated_data.keys()))
            df = st.session_state.generated_data[export_name]
            
            col1, col2 = st.columns(2)
            with col1:
                csv = df.to_csv(index=False).encode("utf-8")
                st.download_button(
                    "📥 Download selected table (CSV)",
                    data=csv,
                    file_name=f"{export_name}.csv",
                    mime="text/csv",
                    use_container_width=True
                )
            
            with col2:
                # Convert to Excel
                output = BytesIO()
                with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                    df.to_excel(writer, index=False, sheet_name='Data')
                
                excel_data = output.getvalue()
                
                st.download_button(
                    "📊 Download as Excel",
                    data=excel_data,
                    file_name=f"{export_name}.xlsx",
                    mime="application/vnd.ms-excel",
                    use_container_width=True
                )
            
            st.caption(f"Table '{export_name}' has {len(df):,} rows and {len(df.columns)} columns.")
        else:
            st.info("No tables loaded to export.")
        
        st.markdown("---")
        col1, col2 = st.columns([1, 1])
        with col1:
            if st.button("🗑️ Clear all loaded tables", type="secondary", use_container_width=True):
                st.session_state.generated_data = {}
                st.success("Cleared all tables from session store.")
                st.rerun()
        
        with col2:
            if st.button("🔄 Reset to Defaults", type="secondary", use_container_width=True):
                st.session_state.generated_data = {}
                st.rerun()
        
        st.warning("Reset does not delete your source files. It only clears the in-app memory store.")
    
    st.markdown("---")
    st.subheader("🏭 Manufacturing Data Overview")
    
    if st.session_state.generated_data:
        # Create summary cards
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            if 'fact_production_batches' in st.session_state.generated_data:
                batches = st.session_state.generated_data['fact_production_batches']
                total_batches = len(batches)
                avg_yield = batches['yield_percentage'].mean()
                st.metric("Production Batches", f"{total_batches:,}", f"Avg Yield: {avg_yield:.1f}%")
        
        with col2:
            if 'dim_inventory_items' in st.session_state.generated_data:
                inventory = st.session_state.generated_data['dim_inventory_items']
                total_value = inventory['total_value'].sum()
                total_items = len(inventory)
                st.metric("Inventory Value", f"KES {total_value:,.0f}", f"{total_items} SKUs")
        
        with col3:
            if 'fact_sales_transactions' in st.session_state.generated_data:
                sales = st.session_state.generated_data['fact_sales_transactions']
                total_sales = sales['net_amount'].sum()
                total_transactions = len(sales)
                st.metric("Total Sales", f"KES {total_sales:,.0f}", f"{total_transactions:,} transactions")
        
        with col4:
            if 'dim_customers' in st.session_state.generated_data:
                customers = st.session_state.generated_data['dim_customers']
                b2b = len(customers[customers['customer_type'] == 'Business'])
                b2c = len(customers[customers['customer_type'] == 'Individual'])
                st.metric("Customers", f"{len(customers):,}", f"B2B: {b2b}, B2C: {b2c}")

if __name__ == "__main__":
    main()