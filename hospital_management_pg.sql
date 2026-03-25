-- ============================================================
--   HOSPITAL MANAGEMENT SYSTEM — PostgreSQL Schema
--   Converted from MySQL v1.0
--   Compatible with: PostgreSQL 14+
-- ============================================================

-- ============================================================
-- 1. DATABASE SETUP
-- Run this first in psql:
--   CREATE DATABASE hospital_db;
--   \c hospital_db
-- ============================================================

-- Disable triggers temporarily for clean bulk load
-- SET session_replication_role = replica;


-- ============================================================
-- 2. CORE TABLES
-- ============================================================

-- 2.1 DEPARTMENTS
CREATE TABLE IF NOT EXISTS departments (
    department_id   SERIAL PRIMARY KEY,
    name            VARCHAR(100) NOT NULL,
    description     TEXT,
    location        VARCHAR(100),
    head_doctor_id  INTEGER DEFAULT NULL,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 2.2 DOCTORS
CREATE TABLE IF NOT EXISTS doctors (
    doctor_id        SERIAL PRIMARY KEY,
    first_name       VARCHAR(60) NOT NULL,
    last_name        VARCHAR(60) NOT NULL,
    email            VARCHAR(120) UNIQUE NOT NULL,
    phone            VARCHAR(20),
    specialization   VARCHAR(100),
    department_id    INTEGER,
    license_number   VARCHAR(50) UNIQUE NOT NULL,
    consultation_fee DECIMAL(10,2) DEFAULT 0.00,
    available_from   TIME DEFAULT '09:00:00',
    available_to     TIME DEFAULT '17:00:00',
    is_active        BOOLEAN DEFAULT TRUE,
    joined_date      DATE,
    created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_doctor_dept
        FOREIGN KEY (department_id) REFERENCES departments(department_id)
        ON DELETE SET NULL ON UPDATE CASCADE
);

-- Deferred FK on departments → doctors
ALTER TABLE departments
    ADD CONSTRAINT fk_dept_head
        FOREIGN KEY (head_doctor_id) REFERENCES doctors(doctor_id)
        ON DELETE SET NULL;

-- 2.3 PATIENTS
CREATE TABLE IF NOT EXISTS patients (
    patient_id              SERIAL PRIMARY KEY,
    first_name              VARCHAR(60) NOT NULL,
    last_name               VARCHAR(60) NOT NULL,
    date_of_birth           DATE NOT NULL,
    gender                  VARCHAR(10) NOT NULL CHECK (gender IN ('Male','Female','Other')),
    blood_group             VARCHAR(5)  CHECK (blood_group IN ('A+','A-','B+','B-','AB+','AB-','O+','O-')),
    email                   VARCHAR(120) UNIQUE,
    phone                   VARCHAR(20) NOT NULL,
    address                 TEXT,
    emergency_contact_name  VARCHAR(120),
    emergency_contact_phone VARCHAR(20),
    allergies               TEXT,
    created_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Trigger to auto-update updated_at (replaces MySQL ON UPDATE CURRENT_TIMESTAMP)
CREATE OR REPLACE FUNCTION fn_set_updated_at()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$;

CREATE TRIGGER trg_patients_updated_at
    BEFORE UPDATE ON patients
    FOR EACH ROW EXECUTE FUNCTION fn_set_updated_at();

CREATE INDEX idx_patient_name  ON patients (last_name, first_name);
CREATE INDEX idx_patient_phone ON patients (phone);

-- 2.4 STAFF
CREATE TABLE IF NOT EXISTS staff (
    staff_id      SERIAL PRIMARY KEY,
    first_name    VARCHAR(60) NOT NULL,
    last_name     VARCHAR(60) NOT NULL,
    role          VARCHAR(20) NOT NULL CHECK (role IN ('Nurse','Receptionist','Pharmacist','Lab Technician','Admin')),
    department_id INTEGER,
    email         VARCHAR(120) UNIQUE NOT NULL,
    phone         VARCHAR(20),
    is_active     BOOLEAN DEFAULT TRUE,
    joined_date   DATE,
    created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_staff_dept
        FOREIGN KEY (department_id) REFERENCES departments(department_id)
        ON DELETE SET NULL
);


-- ============================================================
-- 3. CLINICAL TABLES
-- ============================================================

-- 3.1 APPOINTMENTS
CREATE TABLE IF NOT EXISTS appointments (
    appointment_id   SERIAL PRIMARY KEY,
    patient_id       INTEGER NOT NULL,
    doctor_id        INTEGER NOT NULL,
    appointment_date DATE NOT NULL,
    appointment_time TIME NOT NULL,
    reason           VARCHAR(255),
    status           VARCHAR(20) DEFAULT 'Scheduled'
                     CHECK (status IN ('Scheduled','Confirmed','Completed','Cancelled','No-Show')),
    notes            TEXT,
    created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_appt_patient
        FOREIGN KEY (patient_id) REFERENCES patients(patient_id)
        ON DELETE CASCADE,
    CONSTRAINT fk_appt_doctor
        FOREIGN KEY (doctor_id) REFERENCES doctors(doctor_id)
        ON DELETE RESTRICT,

    -- Prevent double-booking the same doctor at the same slot
    CONSTRAINT uq_doctor_slot UNIQUE (doctor_id, appointment_date, appointment_time)
);

CREATE TRIGGER trg_appointments_updated_at
    BEFORE UPDATE ON appointments
    FOR EACH ROW EXECUTE FUNCTION fn_set_updated_at();

CREATE INDEX idx_appt_date    ON appointments (appointment_date);
CREATE INDEX idx_appt_patient ON appointments (patient_id);
CREATE INDEX idx_appt_doctor  ON appointments (doctor_id);

-- 3.2 MEDICAL RECORDS
CREATE TABLE IF NOT EXISTS medical_records (
    record_id       SERIAL PRIMARY KEY,
    patient_id      INTEGER NOT NULL,
    doctor_id       INTEGER NOT NULL,
    appointment_id  INTEGER,
    visit_date      DATE NOT NULL,
    chief_complaint TEXT,
    diagnosis       TEXT,
    treatment_plan  TEXT,
    blood_pressure  VARCHAR(20),
    heart_rate      SMALLINT,
    temperature     DECIMAL(4,1),
    weight_kg       DECIMAL(5,2),
    height_cm       DECIMAL(5,2),
    notes           TEXT,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_rec_patient
        FOREIGN KEY (patient_id) REFERENCES patients(patient_id)
        ON DELETE CASCADE,
    CONSTRAINT fk_rec_doctor
        FOREIGN KEY (doctor_id) REFERENCES doctors(doctor_id)
        ON DELETE RESTRICT,
    CONSTRAINT fk_rec_appt
        FOREIGN KEY (appointment_id) REFERENCES appointments(appointment_id)
        ON DELETE SET NULL
);

CREATE INDEX idx_rec_patient ON medical_records (patient_id);
CREATE INDEX idx_rec_visit   ON medical_records (visit_date);

-- 3.3 PRESCRIPTIONS
CREATE TABLE IF NOT EXISTS prescriptions (
    prescription_id SERIAL PRIMARY KEY,
    record_id       INTEGER NOT NULL,
    patient_id      INTEGER NOT NULL,
    doctor_id       INTEGER NOT NULL,
    prescribed_date DATE NOT NULL,
    valid_till      DATE,
    notes           TEXT,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_presc_record
        FOREIGN KEY (record_id) REFERENCES medical_records(record_id)
        ON DELETE CASCADE,
    CONSTRAINT fk_presc_patient
        FOREIGN KEY (patient_id) REFERENCES patients(patient_id)
        ON DELETE CASCADE,
    CONSTRAINT fk_presc_doctor
        FOREIGN KEY (doctor_id) REFERENCES doctors(doctor_id)
        ON DELETE RESTRICT
);

-- 3.4 PRESCRIPTION ITEMS
-- Note: medicines table referenced here — created in section 4 below.
-- FK added after medicines table via ALTER.
CREATE TABLE IF NOT EXISTS prescription_items (
    item_id         SERIAL PRIMARY KEY,
    prescription_id INTEGER NOT NULL,
    medicine_id     INTEGER NOT NULL,
    dosage          VARCHAR(50),
    frequency       VARCHAR(50),
    duration_days   SMALLINT,
    quantity        SMALLINT DEFAULT 1,
    instructions    VARCHAR(255),

    CONSTRAINT fk_pi_prescription
        FOREIGN KEY (prescription_id) REFERENCES prescriptions(prescription_id)
        ON DELETE CASCADE
);

-- 3.5 LAB TESTS (catalog)
CREATE TABLE IF NOT EXISTS lab_tests (
    test_id      SERIAL PRIMARY KEY,
    test_name    VARCHAR(150) NOT NULL,
    category     VARCHAR(100),
    normal_range VARCHAR(100),
    unit         VARCHAR(30),
    base_price   DECIMAL(10,2) DEFAULT 0.00,
    is_active    BOOLEAN DEFAULT TRUE
);

-- 3.6 LAB RESULTS
CREATE TABLE IF NOT EXISTS lab_results (
    result_id    SERIAL PRIMARY KEY,
    patient_id   INTEGER NOT NULL,
    record_id    INTEGER,
    test_id      INTEGER NOT NULL,
    ordered_by   INTEGER NOT NULL,
    ordered_date DATE NOT NULL,
    result_date  DATE,
    result_value VARCHAR(200),
    status       VARCHAR(20) DEFAULT 'Ordered'
                 CHECK (status IN ('Ordered','In Progress','Completed','Cancelled')),
    remarks      TEXT,
    created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_lr_patient
        FOREIGN KEY (patient_id) REFERENCES patients(patient_id)
        ON DELETE CASCADE,
    CONSTRAINT fk_lr_record
        FOREIGN KEY (record_id) REFERENCES medical_records(record_id)
        ON DELETE SET NULL,
    CONSTRAINT fk_lr_test
        FOREIGN KEY (test_id) REFERENCES lab_tests(test_id)
        ON DELETE RESTRICT,
    CONSTRAINT fk_lr_doctor
        FOREIGN KEY (ordered_by) REFERENCES doctors(doctor_id)
        ON DELETE RESTRICT
);

CREATE INDEX idx_lr_patient ON lab_results (patient_id);
CREATE INDEX idx_lr_date    ON lab_results (ordered_date);


-- ============================================================
-- 4. INVENTORY TABLES
-- ============================================================

-- 4.1 MEDICINES
CREATE TABLE IF NOT EXISTS medicines (
    medicine_id              SERIAL PRIMARY KEY,
    name                     VARCHAR(150) NOT NULL,
    generic_name             VARCHAR(150),
    category                 VARCHAR(100),
    manufacturer             VARCHAR(150),
    unit_price               DECIMAL(10,2) NOT NULL DEFAULT 0.00,
    requires_prescription    BOOLEAN DEFAULT TRUE,
    is_active                BOOLEAN DEFAULT TRUE,
    created_at               TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_med_name ON medicines (name);

-- Add deferred FK now that medicines exists
ALTER TABLE prescription_items
    ADD CONSTRAINT fk_pi_medicine
        FOREIGN KEY (medicine_id) REFERENCES medicines(medicine_id)
        ON DELETE RESTRICT;

-- 4.2 MEDICINE STOCK
CREATE TABLE IF NOT EXISTS medicine_stock (
    stock_id      SERIAL PRIMARY KEY,
    medicine_id   INTEGER NOT NULL UNIQUE,
    quantity      INTEGER DEFAULT 0,
    reorder_level INTEGER DEFAULT 50,
    expiry_date   DATE,
    last_updated  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_stock_medicine
        FOREIGN KEY (medicine_id) REFERENCES medicines(medicine_id)
        ON DELETE CASCADE
);

CREATE OR REPLACE FUNCTION fn_stock_updated_at()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    NEW.last_updated = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$;

CREATE TRIGGER trg_stock_updated_at
    BEFORE UPDATE ON medicine_stock
    FOR EACH ROW EXECUTE FUNCTION fn_stock_updated_at();


-- ============================================================
-- 5. BILLING TABLES
-- ============================================================

-- 5.1 BILLS
CREATE TABLE IF NOT EXISTS bills (
    bill_id        SERIAL PRIMARY KEY,
    patient_id     INTEGER NOT NULL,
    appointment_id INTEGER,
    bill_date      DATE NOT NULL,
    due_date       DATE,
    subtotal       DECIMAL(12,2) DEFAULT 0.00,
    discount_pct   DECIMAL(5,2)  DEFAULT 0.00,
    tax_pct        DECIMAL(5,2)  DEFAULT 13.00,
    total_amount   DECIMAL(12,2) DEFAULT 0.00,
    amount_paid    DECIMAL(12,2) DEFAULT 0.00,
    status         VARCHAR(20) DEFAULT 'Draft'
                   CHECK (status IN ('Draft','Issued','Partially Paid','Paid','Cancelled')),
    notes          TEXT,
    created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_bill_patient
        FOREIGN KEY (patient_id) REFERENCES patients(patient_id)
        ON DELETE RESTRICT,
    CONSTRAINT fk_bill_appt
        FOREIGN KEY (appointment_id) REFERENCES appointments(appointment_id)
        ON DELETE SET NULL
);

CREATE TRIGGER trg_bills_updated_at
    BEFORE UPDATE ON bills
    FOR EACH ROW EXECUTE FUNCTION fn_set_updated_at();

CREATE INDEX idx_bill_patient ON bills (patient_id);
CREATE INDEX idx_bill_date    ON bills (bill_date);

-- 5.2 BILL ITEMS
-- PostgreSQL GENERATED ALWAYS AS syntax
CREATE TABLE IF NOT EXISTS bill_items (
    bill_item_id SERIAL PRIMARY KEY,
    bill_id      INTEGER NOT NULL,
    item_type    VARCHAR(20) NOT NULL
                 CHECK (item_type IN ('Consultation','Lab Test','Medicine','Procedure','Room','Other')),
    description  VARCHAR(255) NOT NULL,
    quantity     SMALLINT DEFAULT 1,
    unit_price   DECIMAL(10,2) NOT NULL,
    line_total   DECIMAL(12,2) GENERATED ALWAYS AS (quantity * unit_price) STORED,

    CONSTRAINT fk_bi_bill
        FOREIGN KEY (bill_id) REFERENCES bills(bill_id)
        ON DELETE CASCADE
);

-- 5.3 PAYMENTS
CREATE TABLE IF NOT EXISTS payments (
    payment_id   SERIAL PRIMARY KEY,
    bill_id      INTEGER NOT NULL,
    payment_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    amount       DECIMAL(12,2) NOT NULL,
    method       VARCHAR(20) NOT NULL
                 CHECK (method IN ('Cash','Card','Bank Transfer','Mobile Payment','Insurance')),
    reference_no VARCHAR(100),
    received_by  INTEGER,
    notes        TEXT,

    CONSTRAINT fk_pay_bill
        FOREIGN KEY (bill_id) REFERENCES bills(bill_id)
        ON DELETE RESTRICT,
    CONSTRAINT fk_pay_staff
        FOREIGN KEY (received_by) REFERENCES staff(staff_id)
        ON DELETE SET NULL
);

CREATE INDEX idx_pay_bill  ON payments (bill_id);
CREATE INDEX idx_pay_date  ON payments (payment_date);

-- Re-enable triggers
SET session_replication_role = DEFAULT;


-- ============================================================
-- 6. VIEWS  (logic identical — only CURDATE() → CURRENT_DATE,
--            TIMESTAMPDIFF → EXTRACT, DATE_FORMAT → TO_CHAR)
-- ============================================================

-- 6.1 Today's appointment schedule
CREATE OR REPLACE VIEW vw_todays_appointments AS
SELECT
    a.appointment_id,
    a.appointment_time,
    p.first_name || ' ' || p.last_name     AS patient_name,
    p.phone                                 AS patient_phone,
    d.first_name || ' ' || d.last_name     AS doctor_name,
    d.specialization,
    dep.name                                AS department,
    a.reason,
    a.status
FROM appointments a
JOIN patients     p   ON a.patient_id    = p.patient_id
JOIN doctors      d   ON a.doctor_id     = d.doctor_id
LEFT JOIN departments dep ON d.department_id = dep.department_id
WHERE a.appointment_date = CURRENT_DATE
ORDER BY a.appointment_time;

-- 6.2 Patient summary card
CREATE OR REPLACE VIEW vw_patient_summary AS
SELECT
    p.patient_id,
    p.first_name || ' ' || p.last_name           AS full_name,
    p.date_of_birth,
    EXTRACT(YEAR FROM AGE(p.date_of_birth))::INT  AS age,
    p.gender,
    p.blood_group,
    p.phone,
    p.allergies,
    COUNT(DISTINCT a.appointment_id)              AS total_appointments,
    COUNT(DISTINCT mr.record_id)                  AS total_visits,
    MAX(a.appointment_date)                       AS last_visit_date
FROM patients p
LEFT JOIN appointments    a  ON p.patient_id = a.patient_id
LEFT JOIN medical_records mr ON p.patient_id = mr.patient_id
GROUP BY p.patient_id, p.first_name, p.last_name, p.date_of_birth,
         p.gender, p.blood_group, p.phone, p.allergies;

-- 6.3 Outstanding bills
CREATE OR REPLACE VIEW vw_outstanding_bills AS
SELECT
    b.bill_id,
    p.first_name || ' ' || p.last_name     AS patient_name,
    p.phone,
    b.bill_date,
    b.due_date,
    b.total_amount,
    b.amount_paid,
    (b.total_amount - b.amount_paid)        AS balance_due,
    b.status,
    (CURRENT_DATE - b.due_date)             AS days_overdue
FROM bills b
JOIN patients p ON b.patient_id = p.patient_id
WHERE b.status IN ('Issued','Partially Paid')
ORDER BY days_overdue DESC;

-- 6.4 Doctor revenue report
CREATE OR REPLACE VIEW vw_doctor_revenue AS
SELECT
    d.doctor_id,
    d.first_name || ' ' || d.last_name     AS doctor_name,
    d.specialization,
    dep.name                                AS department,
    COUNT(DISTINCT a.appointment_id)        AS total_appointments,
    SUM(bi.line_total)                      AS total_revenue
FROM doctors d
LEFT JOIN departments  dep ON d.department_id  = dep.department_id
LEFT JOIN appointments a   ON d.doctor_id      = a.doctor_id
                          AND a.status         = 'Completed'
LEFT JOIN bills        b   ON a.appointment_id = b.appointment_id
LEFT JOIN bill_items   bi  ON b.bill_id        = bi.bill_id
                          AND bi.item_type     = 'Consultation'
GROUP BY d.doctor_id, d.first_name, d.last_name, d.specialization, dep.name
ORDER BY total_revenue DESC;

-- 6.5 Low medicine stock alert
CREATE OR REPLACE VIEW vw_low_stock AS
SELECT
    m.medicine_id,
    m.name          AS medicine_name,
    m.category,
    s.quantity      AS current_stock,
    s.reorder_level,
    s.expiry_date,
    CASE
        WHEN s.quantity = 0              THEN 'OUT OF STOCK'
        WHEN s.quantity <= s.reorder_level THEN 'LOW STOCK'
        ELSE 'OK'
    END             AS stock_status
FROM medicines m
JOIN medicine_stock s ON m.medicine_id = s.medicine_id
WHERE s.quantity <= s.reorder_level
ORDER BY s.quantity ASC;


-- ============================================================
-- 7. FUNCTIONS (replaces MySQL stored procedures)
--    PostgreSQL uses PL/pgSQL functions instead of PROCEDURE.
-- ============================================================

-- 7.1 Book an appointment
CREATE OR REPLACE FUNCTION fn_book_appointment(
    p_patient_id  INTEGER,
    p_doctor_id   INTEGER,
    p_date        DATE,
    p_time        TIME,
    p_reason      VARCHAR
) RETURNS TABLE (appointment_id INTEGER, message TEXT)
LANGUAGE plpgsql AS $$
DECLARE
    v_conflict  INTEGER := 0;
    v_active    BOOLEAN;
    v_appt_id   INTEGER;
BEGIN
    SELECT is_active INTO v_active FROM doctors WHERE doctor_id = p_doctor_id;

    IF v_active IS NULL THEN
        RETURN QUERY SELECT 0, 'Error: Doctor not found.'::TEXT;
        RETURN;
    ELSIF v_active = FALSE THEN
        RETURN QUERY SELECT 0, 'Error: Doctor is not currently active.'::TEXT;
        RETURN;
    END IF;

    SELECT COUNT(*) INTO v_conflict
    FROM appointments
    WHERE doctor_id        = p_doctor_id
      AND appointment_date = p_date
      AND appointment_time = p_time
      AND status NOT IN ('Cancelled','No-Show');

    IF v_conflict > 0 THEN
        RETURN QUERY SELECT 0, 'Error: This time slot is already booked.'::TEXT;
        RETURN;
    END IF;

    INSERT INTO appointments (patient_id, doctor_id, appointment_date, appointment_time, reason, status)
    VALUES (p_patient_id, p_doctor_id, p_date, p_time, p_reason, 'Scheduled')
    RETURNING appointments.appointment_id INTO v_appt_id;

    RETURN QUERY SELECT v_appt_id, ('Success: Appointment #' || v_appt_id || ' booked.')::TEXT;
END;
$$;

-- 7.2 Generate a bill
CREATE OR REPLACE FUNCTION fn_generate_bill(
    p_appointment_id INTEGER,
    p_discount_pct   DECIMAL
) RETURNS TABLE (bill_id INTEGER, total_amount DECIMAL)
LANGUAGE plpgsql AS $$
DECLARE
    v_patient_id   INTEGER;
    v_consult_fee  DECIMAL(10,2);
    v_subtotal     DECIMAL(12,2);
    v_tax          DECIMAL(5,2) := 13.00;
    v_due_date     DATE;
    v_bill_id      INTEGER;
    v_total        DECIMAL(12,2);
BEGIN
    SELECT a.patient_id, d.consultation_fee
    INTO   v_patient_id, v_consult_fee
    FROM appointments a
    JOIN doctors d ON a.doctor_id = d.doctor_id
    WHERE a.appointment_id = p_appointment_id;

    v_due_date := CURRENT_DATE + INTERVAL '30 days';

    INSERT INTO bills (patient_id, appointment_id, bill_date, due_date, discount_pct, tax_pct, status)
    VALUES (v_patient_id, p_appointment_id, CURRENT_DATE, v_due_date, p_discount_pct, v_tax, 'Draft')
    RETURNING bills.bill_id INTO v_bill_id;

    INSERT INTO bill_items (bill_id, item_type, description, quantity, unit_price)
    VALUES (v_bill_id, 'Consultation', 'Doctor consultation fee', 1, v_consult_fee);

    INSERT INTO bill_items (bill_id, item_type, description, quantity, unit_price)
    SELECT v_bill_id, 'Lab Test', lt.test_name, 1, lt.base_price
    FROM lab_results lr
    JOIN lab_tests lt ON lr.test_id = lt.test_id
    JOIN medical_records mr ON lr.record_id = mr.record_id
    WHERE mr.appointment_id = p_appointment_id AND lr.status != 'Cancelled';

    SELECT SUM(bi.line_total) INTO v_subtotal FROM bill_items bi WHERE bi.bill_id = v_bill_id;

    UPDATE bills
    SET subtotal     = v_subtotal,
        total_amount = v_subtotal * (1 - p_discount_pct / 100) * (1 + v_tax / 100),
        status       = 'Issued'
    WHERE bills.bill_id = v_bill_id;

    SELECT b.total_amount INTO v_total FROM bills b WHERE b.bill_id = v_bill_id;

    RETURN QUERY SELECT v_bill_id, v_total;
END;
$$;

-- 7.3 Record a payment
CREATE OR REPLACE FUNCTION fn_record_payment(
    p_bill_id    INTEGER,
    p_amount     DECIMAL,
    p_method     VARCHAR,
    p_reference  VARCHAR,
    p_staff_id   INTEGER
) RETURNS TABLE (status VARCHAR, balance DECIMAL)
LANGUAGE plpgsql AS $$
DECLARE
    v_total    DECIMAL(12,2);
    v_paid     DECIMAL(12,2);
    v_new_paid DECIMAL(12,2);
    v_status   VARCHAR(20);
    v_balance  DECIMAL(12,2);
BEGIN
    SELECT b.total_amount, b.amount_paid INTO v_total, v_paid
    FROM bills b WHERE b.bill_id = p_bill_id;

    v_new_paid := v_paid + p_amount;

    INSERT INTO payments (bill_id, amount, method, reference_no, received_by)
    VALUES (p_bill_id, p_amount, p_method, p_reference, p_staff_id);

    IF v_new_paid >= v_total THEN
        v_status := 'Paid';
    ELSIF v_new_paid > 0 THEN
        v_status := 'Partially Paid';
    ELSE
        v_status := 'Issued';
    END IF;

    UPDATE bills SET amount_paid = v_new_paid, status = v_status
    WHERE bills.bill_id = p_bill_id;

    v_balance := GREATEST(v_total - v_new_paid, 0);
    RETURN QUERY SELECT v_status, v_balance;
END;
$$;

-- 7.4 Monthly revenue summary
CREATE OR REPLACE FUNCTION fn_monthly_revenue(p_year INTEGER, p_month INTEGER)
RETURNS TABLE (
    month                TEXT,
    total_bills          BIGINT,
    gross_revenue        DECIMAL,
    collected            DECIMAL,
    outstanding          DECIMAL,
    consultation_revenue DECIMAL,
    lab_revenue          DECIMAL,
    pharmacy_revenue     DECIMAL
) LANGUAGE plpgsql AS $$
BEGIN
    RETURN QUERY
    SELECT
        TO_CHAR(b.bill_date, 'YYYY-MM'),
        COUNT(DISTINCT b.bill_id),
        SUM(b.total_amount),
        SUM(b.amount_paid),
        SUM(b.total_amount - b.amount_paid),
        SUM(CASE WHEN bi.item_type = 'Consultation' THEN bi.line_total ELSE 0 END),
        SUM(CASE WHEN bi.item_type = 'Lab Test'     THEN bi.line_total ELSE 0 END),
        SUM(CASE WHEN bi.item_type = 'Medicine'     THEN bi.line_total ELSE 0 END)
    FROM bills b
    JOIN bill_items bi ON b.bill_id = bi.bill_id
    WHERE EXTRACT(YEAR  FROM b.bill_date) = p_year
      AND EXTRACT(MONTH FROM b.bill_date) = p_month
      AND b.status != 'Cancelled'
    GROUP BY TO_CHAR(b.bill_date, 'YYYY-MM');
END;
$$;


-- ============================================================
-- 8. TRIGGERS
-- ============================================================

-- 8.1 Auto-deduct medicine stock on prescription
CREATE OR REPLACE FUNCTION fn_deduct_stock()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    UPDATE medicine_stock
    SET quantity = GREATEST(quantity - NEW.quantity, 0)
    WHERE medicine_id = NEW.medicine_id;
    RETURN NEW;
END;
$$;

CREATE TRIGGER trg_deduct_stock
    AFTER INSERT ON prescription_items
    FOR EACH ROW EXECUTE FUNCTION fn_deduct_stock();

-- 8.2 Auto-update bill status when payment received
CREATE OR REPLACE FUNCTION fn_payment_status()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE
    v_total DECIMAL(12,2);
    v_paid  DECIMAL(12,2);
BEGIN
    SELECT total_amount, amount_paid INTO v_total, v_paid
    FROM bills WHERE bill_id = NEW.bill_id;

    IF v_paid >= v_total THEN
        UPDATE bills SET status = 'Paid' WHERE bill_id = NEW.bill_id;
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER trg_payment_status
    AFTER INSERT ON payments
    FOR EACH ROW EXECUTE FUNCTION fn_payment_status();

-- 8.3 Mark appointment Completed when medical record is created
CREATE OR REPLACE FUNCTION fn_complete_appointment()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.appointment_id IS NOT NULL THEN
        UPDATE appointments SET status = 'Completed'
        WHERE appointment_id = NEW.appointment_id;
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER trg_complete_appointment
    AFTER INSERT ON medical_records
    FOR EACH ROW EXECUTE FUNCTION fn_complete_appointment();

-- 8.4 Prevent booking in the past
CREATE OR REPLACE FUNCTION fn_no_past_appointments()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.appointment_date < CURRENT_DATE THEN
        RAISE EXCEPTION 'Cannot book an appointment in the past.';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER trg_no_past_appointments
    BEFORE INSERT ON appointments
    FOR EACH ROW EXECUTE FUNCTION fn_no_past_appointments();


-- ============================================================
-- 9. SAMPLE DATA  (identical to MySQL version)
-- ============================================================

INSERT INTO departments (name, description, location) VALUES
('General Medicine',  'Primary care and internal medicine', 'Block A, Floor 1'),
('Cardiology',        'Heart and cardiovascular care',       'Block B, Floor 2'),
('Orthopaedics',      'Bone and joint care',                 'Block A, Floor 2'),
('Pathology & Lab',   'Diagnostic laboratory services',      'Block C, Ground'),
('Pharmacy',          'Medicine dispensing',                 'Block A, Ground');

INSERT INTO doctors (first_name, last_name, email, phone, specialization, department_id, license_number, consultation_fee, joined_date) VALUES
('Ramesh',  'Sharma',  'ramesh.sharma@hospital.np',  '9841000001', 'General Physician',   1, 'NMC-001', 500.00,  '2018-03-01'),
('Sunita',  'Thapa',   'sunita.thapa@hospital.np',   '9841000002', 'Cardiologist',        2, 'NMC-002', 1500.00, '2019-07-15'),
('Bikash',  'Acharya', 'bikash.acharya@hospital.np', '9841000003', 'Orthopaedic Surgeon', 3, 'NMC-003', 1200.00, '2020-01-10'),
('Pratima', 'Rai',     'pratima.rai@hospital.np',    '9841000004', 'Pathologist',         4, 'NMC-004', 800.00,  '2017-06-20');

UPDATE departments SET head_doctor_id = 1 WHERE department_id = 1;
UPDATE departments SET head_doctor_id = 2 WHERE department_id = 2;
UPDATE departments SET head_doctor_id = 3 WHERE department_id = 3;

INSERT INTO staff (first_name, last_name, role, department_id, email, phone, joined_date) VALUES
('Anita',  'Gurung', 'Receptionist', 1, 'anita.g@hospital.np',   '9841000010', '2021-01-05'),
('Roshan', 'Basnet', 'Pharmacist',   5, 'roshan.b@hospital.np',  '9841000011', '2020-09-01'),
('Kamala', 'KC',     'Nurse',        1, 'kamala.kc@hospital.np', '9841000012', '2022-03-15');

INSERT INTO patients (first_name, last_name, date_of_birth, gender, blood_group, email, phone, address, allergies) VALUES
('Aarav',  'Pandey',   '1990-05-12', 'Male',   'B+',  'aarav.pandey@gmail.com', '9851000001', 'Kathmandu, Baneshwor',    'Penicillin'),
('Priya',  'Shrestha', '1985-11-23', 'Female', 'O+',  'priya.shr@gmail.com',    '9851000002', 'Lalitpur, Pulchowk',      NULL),
('Sanjay', 'Joshi',    '1972-07-30', 'Male',   'A-',  'sanjay.joshi@gmail.com', '9851000003', 'Bhaktapur, Suryabinayak', NULL),
('Meena',  'Tamang',   '2000-03-18', 'Female', 'AB+', 'meena.tamang@gmail.com', '9851000004', 'Kathmandu, Chabahil',     'Sulfa drugs');

INSERT INTO medicines (name, generic_name, category, manufacturer, unit_price) VALUES
('Paracetamol 500mg', 'Paracetamol',  'Analgesic',    'Nepal Pharma', 5.00),
('Amoxicillin 500mg', 'Amoxicillin',  'Antibiotic',   'Nepal Pharma', 12.00),
('Atorvastatin 10mg', 'Atorvastatin', 'Statin',       'Astra Nepal',  25.00),
('Metformin 500mg',   'Metformin',    'Antidiabetic', 'Astra Nepal',  8.00),
('Omeprazole 20mg',   'Omeprazole',   'PPI',          'Nepal Pharma', 10.00);

INSERT INTO medicine_stock (medicine_id, quantity, reorder_level, expiry_date) VALUES
(1, 500, 100, '2026-12-31'),
(2, 200,  50, '2026-06-30'),
(3,  40,  50, '2027-01-31'),
(4, 300,  75, '2026-09-30'),
(5, 150,  50, '2026-11-30');

INSERT INTO lab_tests (test_name, category, normal_range, unit, base_price) VALUES
('Complete Blood Count',      'Haematology',  'Varies',   '',      300.00),
('Fasting Blood Sugar',       'Biochemistry', '70-100',   'mg/dL', 150.00),
('Lipid Profile',             'Biochemistry', 'Total<200','mg/dL', 500.00),
('Urine Routine Examination', 'Urinalysis',   'Normal',   '',      100.00),
('Serum Creatinine',          'Biochemistry', '0.6-1.2',  'mg/dL', 200.00);


-- ============================================================
-- SAMPLE USAGE — call functions
-- ============================================================

/*
-- Book an appointment
SELECT * FROM fn_book_appointment(1, 1, CURRENT_DATE + 1, '10:00:00', 'Fever and cough');

-- Generate a bill
SELECT * FROM fn_generate_bill(1, 0.00);

-- Record a payment
SELECT * FROM fn_record_payment(1, 500.00, 'Cash', NULL, 1);

-- Monthly revenue
SELECT * FROM fn_monthly_revenue(2026, 3);

-- Query views
SELECT * FROM vw_todays_appointments;
SELECT * FROM vw_outstanding_bills;
SELECT * FROM vw_low_stock;
SELECT * FROM vw_doctor_revenue;
SELECT * FROM vw_patient_summary WHERE patient_id = 1;
*/

-- ============================================================
--   END OF SCHEMA
-- ============================================================
