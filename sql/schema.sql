
-- 1. PRIORIDAD 1: Tablas sin dependencias (customer, parts)
CREATE TABLE customer (
    customer_id SERIAL PRIMARY KEY,
    customer_number VARCHAR(40),
    customer_name VARCHAR(50) NOT NULL,
    creation_date DATE,
    department_city VARCHAR(50),
    address VARCHAR(50),
    phone VARCHAR(20),
    email VARCHAR(50) NOT NULL UNIQUE,
    status VARCHAR(20)
);

CREATE TABLE parts (
    part_id SERIAL PRIMARY KEY,
    part_number VARCHAR(50),
    description VARCHAR(70) NOT NULL,
    category VARCHAR(50),
    unit_cost NUMERIC(10,2),
    supplier VARCHAR(50),
    stock_qty INTEGER NOT NULL,
    min_stock INTEGER NOT NULL
);

-- 2. PRIORIDAD 2: Tablas que dependen solo de customer/parts
CREATE TABLE equipment (
    equipment_id SERIAL PRIMARY KEY,
    serial_number VARCHAR(50),
    model VARCHAR(50),
    year INTEGER,
    customer_id INTEGER NOT NULL,
    current_status VARCHAR(50),
    hours_worked INTEGER,
    location_department VARCHAR(50),
    location_city VARCHAR(50)
);

CREATE TABLE sales (
    sale_id SERIAL PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    model VARCHAR(50),
    serial_number VARCHAR(100),
    year INTEGER,
    sale_date DATE,
    sale_price NUMERIC(10,2),
    warranty_months INTEGER,
    payment_terms VARCHAR(50),
    delivery_date DATE,
    salesperson VARCHAR(100)
);

-- 3. PRIORIDAD 3: Tablas con múltiples dependencias
CREATE TABLE service_order (
    service_id SERIAL PRIMARY KEY,
    equipment_id INTEGER NOT NULL,
    customer_id INTEGER NOT NULL,
    service_type VARCHAR(40),
    start_date DATE,
    end_date DATE,
    reported_issue VARCHAR(50),
    root_cause VARCHAR(50),
    technician VARCHAR(40),
    status VARCHAR(40)
);

CREATE TABLE failures (
    failure_id SERIAL PRIMARY KEY,
    equipment_id INTEGER NOT NULL,
    failure_date DATE,  -- ✅ Corregido: failure_date
    failure_category VARCHAR(50),
    severity VARCHAR(40),
    description VARCHAR(100)
);

CREATE TABLE warranty_cases (
    warranty_id SERIAL PRIMARY KEY,
    equipment_id INTEGER NOT NULL,
    customer_id INTEGER NOT NULL,
    case_open_date DATE,
    case_close_date DATE,
    issue_description VARCHAR(100),
    approval_status VARCHAR(40),
    amount_claimed NUMERIC(10,2),
    amount_approved NUMERIC(10,2)
);

-- 4. PRIORIDAD 4: Tablas N-N (últimas)
CREATE TABLE parts_usage (
    part_usage_id SERIAL PRIMARY KEY,
    service_id INTEGER NOT NULL,
    part_id INTEGER NOT NULL,
    quantity INTEGER,
    unit_price NUMERIC(10,2),
    total_price NUMERIC(10,2),
    warehouse_location VARCHAR(10)
);

CREATE TABLE parts_sales (
    sale_line_id SERIAL PRIMARY KEY,
    sale_date DATE,
    customer_id INTEGER NOT NULL,
    part_id INTEGER NOT NULL,
    quantity INTEGER,
    unit_price NUMERIC(10,2),
    total_price NUMERIC(10,2),
    sale_type VARCHAR(20),
    service_id INTEGER,  -- ✅ NULL permitido (ventas sin servicio)
    invoice_number VARCHAR(30)
);

-- =====================================================
-- AGREGAR FKs (después de todas las tablas creadas)
-- =====================================================

-- equipment → customer
ALTER TABLE equipment ADD CONSTRAINT fk_equipment_customer
    FOREIGN KEY (customer_id) REFERENCES customer(customer_id);

-- sales → customer  
ALTER TABLE sales ADD CONSTRAINT fk_sales_customer
    FOREIGN KEY (customer_id) REFERENCES customer(customer_id);

-- service_order → equipment + customer
ALTER TABLE service_order ADD CONSTRAINT fk_service_equipment
    FOREIGN KEY (equipment_id) REFERENCES equipment(equipment_id);
ALTER TABLE service_order ADD CONSTRAINT fk_service_customer
    FOREIGN KEY (customer_id) REFERENCES customer(customer_id);

-- failures → equipment
ALTER TABLE failures ADD CONSTRAINT fk_failures_equipment
    FOREIGN KEY (equipment_id) REFERENCES equipment(equipment_id);

-- warranty_cases → equipment + customer
ALTER TABLE warranty_cases ADD CONSTRAINT fk_warranty_equipment
    FOREIGN KEY (equipment_id) REFERENCES equipment(equipment_id);
ALTER TABLE warranty_cases ADD CONSTRAINT fk_warranty_customer
    FOREIGN KEY (customer_id) REFERENCES customer(customer_id);

-- parts_usage → service_order + parts
ALTER TABLE parts_usage ADD CONSTRAINT fk_usage_service
    FOREIGN KEY (service_id) REFERENCES service_order(service_id);
ALTER TABLE parts_usage ADD CONSTRAINT fk_usage_parts
    FOREIGN KEY (part_id) REFERENCES parts(part_id);

-- parts_sales → customer + parts + service_order (opcional)
ALTER TABLE parts_sales ADD CONSTRAINT fk_partsales_customer
    FOREIGN KEY (customer_id) REFERENCES customer(customer_id);
ALTER TABLE parts_sales ADD CONSTRAINT fk_partsales_parts
    FOREIGN KEY (part_id) REFERENCES parts(part_id);
ALTER TABLE parts_sales ADD CONSTRAINT fk_partsales_service
    FOREIGN KEY (service_id) REFERENCES service_order(service_id);

-- Índices para rendimiento
CREATE INDEX idx_customer_sales ON sales(customer_id);
CREATE INDEX idx_equipment_customer ON equipment(customer_id);

-- eliminar columna department_city separar en department 
--y city
ALTER TABLE customer DROP COLUMN department_city ;

ALTER TABLE customer 
ADD COLUMN department VARCHAR(50),
ADD COLUMN city VARCHAR(50)
