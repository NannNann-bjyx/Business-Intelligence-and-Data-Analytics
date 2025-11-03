/* ============================================================
   BI HOTEL — PROFESSOR-STYLE STEPS (PSA → DWH → DM)
   ============================================================ */

/* ------------------------------------------------------------
   STEP 1 (PSA): Create database & select schema
   ------------------------------------------------------------ */
DROP DATABASE IF EXISTS bi_hotel;
CREATE DATABASE bi_hotel;
USE bi_hotel;

/* ------------------------------------------------------------
   STEP 2 (PSA): Create PSA table (raw structure + surrogate PK)
   ------------------------------------------------------------ */
-- PSA schema (drop/create fresh)
DROP TABLE IF EXISTS psa_hotel_bookings;
CREATE TABLE psa_hotel_bookings (
  psa_row_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  hotel VARCHAR(30) DEFAULT NULL,
  is_canceled TINYINT UNSIGNED NOT NULL DEFAULT 0,
  lead_time SMALLINT UNSIGNED DEFAULT NULL,
  arrival_date_year SMALLINT UNSIGNED DEFAULT NULL,
  arrival_date_month VARCHAR(15) DEFAULT NULL,
  arrival_date_week_number TINYINT UNSIGNED DEFAULT NULL,
  arrival_date_day_of_month TINYINT UNSIGNED DEFAULT NULL,
  stays_in_weekend_nights TINYINT UNSIGNED DEFAULT NULL,
  stays_in_week_nights TINYINT UNSIGNED DEFAULT NULL,
  adults TINYINT UNSIGNED DEFAULT NULL,
  children TINYINT UNSIGNED DEFAULT NULL,
  babies TINYINT UNSIGNED DEFAULT NULL,
  meal VARCHAR(20) DEFAULT NULL,
  country VARCHAR(10) DEFAULT NULL,
  market_segment VARCHAR(50) DEFAULT NULL,
  distribution_channel VARCHAR(50) DEFAULT NULL,
  is_repeated_guest TINYINT UNSIGNED NOT NULL DEFAULT 0,
  previous_cancellations TINYINT UNSIGNED DEFAULT NULL,
  previous_bookings_not_canceled TINYINT UNSIGNED DEFAULT NULL,
  reserved_room_type VARCHAR(5) DEFAULT NULL,
  assigned_room_type VARCHAR(5) DEFAULT NULL,
  booking_changes TINYINT UNSIGNED DEFAULT NULL,
  deposit_type VARCHAR(20) DEFAULT NULL,
  agent INT UNSIGNED DEFAULT NULL,
  company INT UNSIGNED DEFAULT NULL,
  days_in_waiting_list SMALLINT UNSIGNED DEFAULT NULL,
  customer_type VARCHAR(30) DEFAULT NULL,
  adr DECIMAL(10,4) DEFAULT NULL,
  required_car_parking_spaces TINYINT UNSIGNED DEFAULT NULL,
  total_of_special_requests TINYINT UNSIGNED DEFAULT NULL,
  reservation_status VARCHAR(20) DEFAULT NULL,
  reservation_status_date VARCHAR(10) DEFAULT NULL,
  load_ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (psa_row_id)
) ENGINE=InnoDB;

/* ------------------------------------------------------------
   STEP 3 (PSA): Load raw CSV 
   ------------------------------------------------------------ */
TRUNCATE TABLE psa_hotel_bookings;

LOAD DATA LOCAL INFILE '/Users/hlathiha/Desktop/BI_LABS/Lab_3/hotel_bookings.csv'
INTO TABLE psa_hotel_bookings
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n'           -- use '\r\n' if your file is Windows-formatted
IGNORE 1 LINES
(hotel, is_canceled, lead_time, arrival_date_year, arrival_date_month,
 arrival_date_week_number, arrival_date_day_of_month, stays_in_weekend_nights,
 stays_in_week_nights, adults, @children_txt, babies, meal, country, market_segment,
 distribution_channel, is_repeated_guest, previous_cancellations,
 previous_bookings_not_canceled, reserved_room_type, assigned_room_type,
 booking_changes, deposit_type, agent, company, days_in_waiting_list,
 customer_type, adr, required_car_parking_spaces, total_of_special_requests,
 reservation_status, reservation_status_date)
SET children = CASE
                 WHEN @children_txt IS NULL THEN NULL
                 WHEN LOWER(TRIM(@children_txt)) IN ('', 'na', 'n/a', 'nan', 'null') THEN NULL
                 ELSE CAST(TRIM(@children_txt) AS UNSIGNED)
               END;

/* ------------------------------------------------------------
   STEP 4 (PSA): Quick rowcount & spot-checks
   ------------------------------------------------------------ */
SELECT COUNT(*) AS psa_rows FROM psa_hotel_bookings;
SELECT hotel, COUNT(*) AS cnt FROM psa_hotel_bookings GROUP BY hotel;

/* ------------------------------------------------------------
   STEP 5 (PSA): Optional data sanity checks (nulls/negatives)
   ------------------------------------------------------------ */
SELECT
  SUM(children IS NULL) AS null_children,
  SUM(agent IS NULL OR agent='') AS null_agent,
  SUM(company IS NULL OR company='') AS null_company
FROM psa_hotel_bookings;

/* ------------------------------------------------------------
   STEP 6 (PSA): Helpful PSA indexes (optional)
   ------------------------------------------------------------ */
CREATE INDEX ix_psa_hotel ON psa_hotel_bookings (hotel);
CREATE INDEX ix_psa_dates ON psa_hotel_bookings (arrival_date_year, arrival_date_month, arrival_date_day_of_month);

/* ------------------------------------------------------------
   STEP 7 (DWH / SILVER): Create DWH table & load from PSA
   - Build arrival_date, compute total_nights, type conversions
   ------------------------------------------------------------ */
DROP TABLE IF EXISTS dwh_hotel_bookings;
CREATE TABLE dwh_hotel_bookings (
  dwh_row_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  hotel VARCHAR(30) DEFAULT NULL,
  is_canceled TINYINT UNSIGNED NOT NULL DEFAULT 0,
  lead_time SMALLINT UNSIGNED DEFAULT NULL,
  arrival_date DATE DEFAULT NULL,
  arrival_date_year SMALLINT UNSIGNED DEFAULT NULL,
  arrival_date_month TINYINT UNSIGNED DEFAULT NULL,
  arrival_date_day_of_month TINYINT UNSIGNED DEFAULT NULL,
  stays_in_weekend_nights TINYINT UNSIGNED DEFAULT NULL,
  stays_in_week_nights TINYINT UNSIGNED DEFAULT NULL,
  total_nights SMALLINT UNSIGNED DEFAULT NULL,
  adults TINYINT UNSIGNED DEFAULT NULL,
  children TINYINT UNSIGNED DEFAULT NULL,
  babies TINYINT UNSIGNED DEFAULT NULL,
  meal VARCHAR(20) DEFAULT NULL,
  country VARCHAR(10) DEFAULT NULL,
  market_segment VARCHAR(50) DEFAULT NULL,
  distribution_channel VARCHAR(50) DEFAULT NULL,
  is_repeated_guest TINYINT UNSIGNED NOT NULL DEFAULT 0,
  reserved_room_type VARCHAR(5) DEFAULT NULL,
  assigned_room_type VARCHAR(5) DEFAULT NULL,
  deposit_type VARCHAR(20) DEFAULT NULL,
  agent INT UNSIGNED DEFAULT NULL,
  company INT UNSIGNED DEFAULT NULL,
  customer_type VARCHAR(30) DEFAULT NULL,
  adr DECIMAL(10,4) DEFAULT NULL,
  reservation_status VARCHAR(20) DEFAULT NULL,
  reservation_status_date DATE DEFAULT NULL,
  load_ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (dwh_row_id)
) ENGINE=InnoDB;

INSERT INTO dwh_hotel_bookings (
  hotel,is_canceled,lead_time,
  arrival_date,arrival_date_year,arrival_date_month,arrival_date_day_of_month,
  stays_in_weekend_nights,stays_in_week_nights,total_nights,
  adults,children,babies,
  meal,country,market_segment,distribution_channel,
  is_repeated_guest,
  reserved_room_type,assigned_room_type,deposit_type,
  agent,company,
  customer_type,adr,
  reservation_status,reservation_status_date,
  load_ts
)

SELECT
  p.hotel,
  p.is_canceled,
  p.lead_time,
  STR_TO_DATE(CONCAT(
      p.arrival_date_year,'-',
      LPAD(ELT(FIELD(LOWER(TRIM(p.arrival_date_month)),'january','february','march','april','may','june','july','august','september','october','november','december'),1,2,3,4,5,6,7,8,9,10,11,12),2,'0'),
      '-',LPAD(p.arrival_date_day_of_month,2,'0')
  ),'%Y-%m-%d') AS arrival_date,
  p.arrival_date_year,
  ELT(FIELD(LOWER(TRIM(p.arrival_date_month)),'january','february','march','april','may','june','july','august','september','october','november','december'),1,2,3,4,5,6,7,8,9,10,11,12) AS arrival_date_month,
  p.arrival_date_day_of_month,
  p.stays_in_weekend_nights,
  p.stays_in_week_nights,
  COALESCE(p.stays_in_weekend_nights,0)+COALESCE(p.stays_in_week_nights,0) AS total_nights,
  p.adults,
  p.children,
  p.babies,
  p.meal,
  p.country,
  p.market_segment,
  p.distribution_channel,
  p.is_repeated_guest,
  p.reserved_room_type,
  p.assigned_room_type,
  p.deposit_type,
  p.agent,
  p.company,
  p.customer_type,
  p.adr,
  p.reservation_status,
  STR_TO_DATE(p.reservation_status_date,'%Y-%m-%d'),
  p.load_ts
FROM psa_hotel_bookings p;

CREATE INDEX ix_dwh_arrival_date ON dwh_hotel_bookings(arrival_date);
CREATE INDEX ix_dwh_hotel ON dwh_hotel_bookings(hotel);
CREATE INDEX ix_dwh_market ON dwh_hotel_bookings(market_segment,distribution_channel);
CREATE INDEX ix_dwh_customer ON dwh_hotel_bookings(country,customer_type,is_repeated_guest);
CREATE INDEX ix_dwh_battr ON dwh_hotel_bookings(meal,reserved_room_type,assigned_room_type,deposit_type,agent,company);

SELECT COUNT(*) AS dwh_rows FROM dwh_hotel_bookings;
SELECT MIN(arrival_date) AS min_arrival,MAX(arrival_date) AS max_arrival,SUM(arrival_date IS NULL) AS null_arrival FROM dwh_hotel_bookings;
SELECT SUM(total_nights<>COALESCE(stays_in_weekend_nights,0)+COALESCE(stays_in_week_nights,0)) AS bad_total_nights FROM dwh_hotel_bookings;

/* ============================================================
   -- STEP 8 (DM / GOLD): DIMENSION TABLES (create + load)
   -- Source: dwh_hotel_bookings
   ============================================================ */

-- ---------- 8.1 DIM HOTEL ----------
DROP TABLE IF EXISTS dim_hotel;
CREATE TABLE dim_hotel (
  hotel_key INT UNSIGNED NOT NULL AUTO_INCREMENT,
  hotel_code VARCHAR(30) NOT NULL,
  load_ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (hotel_key),
  UNIQUE KEY ux_dim_hotel_code (hotel_code)
) ENGINE=InnoDB;

-- Distinct hotels from DWH
INSERT INTO dim_hotel (hotel_code)
SELECT DISTINCT hotel FROM dwh_hotel_bookings WHERE hotel IS NOT NULL;

-- ---------- 8.2 DIM DATE (calendar table built from DWH range) ----------
DROP TABLE IF EXISTS dim_date;
CREATE TABLE dim_date (
  date_key INT UNSIGNED NOT NULL AUTO_INCREMENT,
  full_date DATE NOT NULL,
  year SMALLINT UNSIGNED NOT NULL,
  month_num TINYINT UNSIGNED NOT NULL,
  month_name VARCHAR(10) NOT NULL,
  day_of_month TINYINT UNSIGNED NOT NULL,
  day_of_week TINYINT UNSIGNED NOT NULL,   -- 1=Mon..7=Sun
  week_of_year TINYINT UNSIGNED NOT NULL,
  quarter_num TINYINT UNSIGNED NOT NULL,
  load_ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (date_key),
  UNIQUE KEY ux_dim_date_full (full_date)
) ENGINE=InnoDB;

-- Determine bounds from DWH (fallback if DWH empty)
SET @dmin = (SELECT COALESCE(MIN(arrival_date), DATE('2015-01-01')) FROM dwh_hotel_bookings);
SET @dmax = (SELECT COALESCE(MAX(arrival_date), DATE('2017-12-31')) FROM dwh_hotel_bookings);

-- If you ever need more than 1000 recursive steps, uncomment the next line:
-- SET SESSION cte_max_recursion_depth = 10000;

INSERT INTO dim_date (full_date,year,month_num,month_name,day_of_month,day_of_week,week_of_year,quarter_num)
WITH RECURSIVE dates(d) AS (
  SELECT @dmin
  UNION ALL
  SELECT DATE_ADD(d, INTERVAL 1 DAY) FROM dates WHERE d < @dmax
)
SELECT
  d AS full_date,
  YEAR(d) AS year,
  MONTH(d) AS month_num,
  DATE_FORMAT(d,'%b') AS month_name,
  DAY(d) AS day_of_month,
  (DATE_FORMAT(d,'%u')+0) AS day_of_week,  -- 1=Mon..7=Sun
  WEEKOFYEAR(d) AS week_of_year,
  QUARTER(d) AS quarter_num
FROM dates;

-- Optional Unknown Date row
INSERT IGNORE INTO dim_date (full_date,year,month_num,month_name,day_of_month,day_of_week,week_of_year,quarter_num)
VALUES ('1900-01-01',1900,1,'Jan',1,1,1,1);

-- Check
SELECT COUNT(*) AS dim_date_rows, MIN(full_date) AS min_date, MAX(full_date) AS max_date FROM dim_date;

-- ---------- 8.3 DIM MARKET (segment + channel) ----------
DROP TABLE IF EXISTS dim_market;
CREATE TABLE dim_market (
  market_key INT UNSIGNED NOT NULL AUTO_INCREMENT,
  market_segment VARCHAR(50) DEFAULT NULL,
  distribution_channel VARCHAR(50) DEFAULT NULL,
  load_ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (market_key),
  UNIQUE KEY ux_dim_market_nat (market_segment,distribution_channel)
) ENGINE=InnoDB;

INSERT INTO dim_market (market_segment,distribution_channel)
SELECT DISTINCT market_segment,distribution_channel
FROM dwh_hotel_bookings;

-- ---------- 8.4 DIM CUSTOMER (country + type + repeated flag) ----------
DROP TABLE IF EXISTS dim_customer;
CREATE TABLE dim_customer (
  customer_key INT UNSIGNED NOT NULL AUTO_INCREMENT,
  country VARCHAR(10) DEFAULT NULL,
  customer_type VARCHAR(30) DEFAULT NULL,
  is_repeated_guest TINYINT UNSIGNED NOT NULL DEFAULT 0,
  load_ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (customer_key),
  UNIQUE KEY ux_dim_customer_nat (country,customer_type,is_repeated_guest)
) ENGINE=InnoDB;

INSERT INTO dim_customer (country,customer_type,is_repeated_guest)
SELECT DISTINCT country,customer_type,COALESCE(is_repeated_guest,0)
FROM dwh_hotel_bookings;

-- ---------- 8.5 DIM BOOKING ATTR (meal/rooms/deposit/agent/company) ----------
DROP TABLE IF EXISTS dim_booking_attr;
CREATE TABLE dim_booking_attr (
  booking_attr_key INT UNSIGNED NOT NULL AUTO_INCREMENT,
  meal VARCHAR(20) DEFAULT NULL,
  reserved_room_type VARCHAR(5) DEFAULT NULL,
  assigned_room_type VARCHAR(5) DEFAULT NULL,
  deposit_type VARCHAR(20) DEFAULT NULL,
  agent INT UNSIGNED DEFAULT NULL,
  company INT UNSIGNED DEFAULT NULL,
  load_ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (booking_attr_key),
  UNIQUE KEY ux_dim_battr_nat (meal,reserved_room_type,assigned_room_type,deposit_type,agent,company)
) ENGINE=InnoDB;

INSERT INTO dim_booking_attr (meal,reserved_room_type,assigned_room_type,deposit_type,agent,company)
SELECT DISTINCT meal,reserved_room_type,assigned_room_type,deposit_type,agent,company
FROM dwh_hotel_bookings;

-- ---------- 8.6 Quick sanity checks ----------
SELECT 'dim_hotel' AS dim, COUNT(*) AS rows_count FROM dim_hotel
UNION ALL SELECT 'dim_date', COUNT(*) FROM dim_date
UNION ALL SELECT 'dim_market', COUNT(*) FROM dim_market
UNION ALL SELECT 'dim_customer', COUNT(*) FROM dim_customer
UNION ALL SELECT 'dim_booking_attr', COUNT(*) FROM dim_booking_attr;

-- Duplicates should be zero because of UNIQUE keys (these queries double-check)
SELECT COUNT(*) AS dup_market_pairs
FROM (SELECT market_segment,distribution_channel,COUNT(*) c FROM dim_market GROUP BY 1,2 HAVING c>1) x;

SELECT COUNT(*) AS dup_battr_rows
FROM (SELECT meal,reserved_room_type,assigned_room_type,deposit_type,agent,company,COUNT(*) c
      FROM dim_booking_attr GROUP BY 1,2,3,4,5,6 HAVING c>1) y;


/* ============================================================
   STEP 9 (DM / GOLD): Create FACT table & load from DWH
   ============================================================ */
-- ============================================
-- STEP 9 (DM / GOLD): FACT table (create + load)
-- Source: dwh_hotel_bookings + all dims
-- ============================================

-- 9.1 Create FACT table
DROP TABLE IF EXISTS fact_booking;
CREATE TABLE fact_booking (
  booking_key BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  hotel_key INT UNSIGNED NOT NULL,
  arrival_date_key INT UNSIGNED NOT NULL,
  market_key INT UNSIGNED NOT NULL,
  customer_key INT UNSIGNED NOT NULL,
  booking_attr_key INT UNSIGNED NOT NULL,
  is_canceled TINYINT UNSIGNED NOT NULL,              -- 0/1
  lead_time SMALLINT UNSIGNED DEFAULT NULL,
  stays_weekend_nights TINYINT UNSIGNED DEFAULT NULL,
  stays_week_nights TINYINT UNSIGNED DEFAULT NULL,
  total_nights SMALLINT UNSIGNED DEFAULT NULL,
  adults TINYINT UNSIGNED DEFAULT NULL,
  children TINYINT UNSIGNED DEFAULT NULL,
  babies TINYINT UNSIGNED DEFAULT NULL,
  adr DECIMAL(10,4) DEFAULT NULL,
  est_room_revenue DECIMAL(14,4) DEFAULT NULL,        -- adr * total_nights when not canceled
  load_ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (booking_key),
  CONSTRAINT fk_fact_hotel        FOREIGN KEY (hotel_key)        REFERENCES dim_hotel(hotel_key),
  CONSTRAINT fk_fact_arrival      FOREIGN KEY (arrival_date_key) REFERENCES dim_date(date_key),
  CONSTRAINT fk_fact_market       FOREIGN KEY (market_key)       REFERENCES dim_market(market_key),
  CONSTRAINT fk_fact_customer     FOREIGN KEY (customer_key)     REFERENCES dim_customer(customer_key),
  CONSTRAINT fk_fact_booking_attr FOREIGN KEY (booking_attr_key) REFERENCES dim_booking_attr(booking_attr_key)
) ENGINE=InnoDB;

-- 9.2 Resolve the "Unknown Date" key (for any NULL arrival_date)
SET @unknown_date_key = (SELECT date_key FROM dim_date WHERE full_date='1900-01-01');

-- bulk load fact (disable checks for speed, then re-enable)
SET autocommit = 0;
SET FOREIGN_KEY_CHECKS = 0;
SET UNIQUE_CHECKS = 0;

-- 9.3 Load FACT from DWH by looking up natural keys in dims
INSERT INTO fact_booking (
  hotel_key, arrival_date_key, market_key, customer_key, booking_attr_key,
  is_canceled, lead_time, stays_weekend_nights, stays_week_nights, total_nights,
  adults, children, babies, adr, est_room_revenue
)
SELECT
  dh.hotel_key,
  COALESCE(dd.date_key, @unknown_date_key) AS arrival_date_key,
  dm.market_key,
  dc.customer_key,
  dba.booking_attr_key,
  d.is_canceled,
  d.lead_time,
  d.stays_in_weekend_nights,
  d.stays_in_week_nights,
  d.total_nights,
  d.adults,
  d.children,
  d.babies,
  d.adr,
  CASE WHEN d.is_canceled = 1 THEN 0
       ELSE COALESCE(d.adr,0) * COALESCE(d.total_nights,0)
  END AS est_room_revenue
FROM dwh_hotel_bookings d
JOIN dim_hotel        dh  ON dh.hotel_code = d.hotel
LEFT JOIN dim_date     dd  ON dd.full_date  = d.arrival_date                -- LEFT to allow NULL → unknown key
JOIN dim_market       dm  ON dm.market_segment = d.market_segment
                          AND dm.distribution_channel = d.distribution_channel
JOIN dim_customer     dc  ON dc.country = d.country
                          AND COALESCE(dc.customer_type,'') = COALESCE(d.customer_type,'')
                          AND dc.is_repeated_guest = COALESCE(d.is_repeated_guest,0)
JOIN dim_booking_attr dba ON dba.meal = d.meal
                          AND dba.reserved_room_type = d.reserved_room_type
                          AND dba.assigned_room_type = d.assigned_room_type
                          AND dba.deposit_type = d.deposit_type
                          AND dba.agent   <=> d.agent                       -- NULL-safe equality
                          AND dba.company <=> d.company;                    -- NULL-safe equality

SET UNIQUE_CHECKS = 1;
SET FOREIGN_KEY_CHECKS = 1;
COMMIT;
SET autocommit = 1;

-- 9.4 Helpful indexes on the FACT (speed filters/joins)
CREATE INDEX ix_fact_arrival   ON fact_booking(arrival_date_key);
CREATE INDEX ix_fact_hotel     ON fact_booking(hotel_key);
CREATE INDEX ix_fact_market    ON fact_booking(market_key);
CREATE INDEX ix_fact_customer  ON fact_booking(customer_key);
CREATE INDEX ix_fact_battr     ON fact_booking(booking_attr_key);
CREATE INDEX ix_fact_cancel    ON fact_booking(is_canceled);

-- 9.5 Quick sanity checks
SELECT COUNT(*) AS fact_rows FROM fact_booking;

-- Should be 0
SELECT SUM(total_nights <> COALESCE(stays_weekend_nights,0)+COALESCE(stays_week_nights,0)) AS bad_total FROM fact_booking;

-- Basic profile
SELECT is_canceled, COUNT(*) AS bookings, ROUND(SUM(est_room_revenue),2) AS revenue
FROM fact_booking GROUP BY is_canceled;

-- 9.6 Optional: Resort-only convenience view
DROP VIEW IF EXISTS v_fact_booking_resort;
CREATE VIEW v_fact_booking_resort AS
SELECT f.*
FROM fact_booking f
JOIN dim_hotel h ON h.hotel_key = f.hotel_key
WHERE h.hotel_code = 'Resort Hotel';


/* ------------------------------------------------------------
   GOLD: Resort-only business scope (views)
   ------------------------------------------------------------ */
CREATE OR REPLACE VIEW v_fact_booking_resort AS
SELECT f.*
FROM fact_booking f
JOIN dim_hotel h ON h.hotel_key = f.hotel_key
WHERE h.hotel_code = 'Resort Hotel';

CREATE OR REPLACE VIEW v_booking_active_resort AS
SELECT * FROM v_fact_booking_resort WHERE is_canceled = 0;

/* ------------------------------------------------------------
   (Optional) KPI views for dashboards
   ------------------------------------------------------------ */
CREATE OR REPLACE VIEW v_kpi_adr_month_resort AS
SELECT DATE_FORMAT(d.full_date,'%Y-%m') AS ym,
       ROUND(SUM(f.adr * f.total_nights)/NULLIF(SUM(f.total_nights),0),2) AS adr
FROM v_booking_active_resort f
JOIN dim_date d ON d.date_key = f.arrival_date_key
GROUP BY ym;

CREATE OR REPLACE VIEW v_kpi_cancel_by_channel_resort AS
SELECT m.distribution_channel,
       ROUND(AVG(f.is_canceled)*100,2) AS cancel_pct,
       COUNT(*) AS bookings
FROM v_fact_booking_resort f
JOIN dim_market m ON m.market_key = f.market_key
GROUP BY m.distribution_channel
ORDER BY cancel_pct DESC;

CREATE OR REPLACE VIEW v_kpi_leadtime_buckets_resort AS
SELECT CASE
         WHEN f.lead_time < 7   THEN '<1w'
         WHEN f.lead_time < 30  THEN '1w–1m'
         WHEN f.lead_time < 90  THEN '1–3m'
         WHEN f.lead_time < 180 THEN '3–6m'
         ELSE '6m+'
       END AS bucket,
       COUNT(*) AS bookings
FROM v_booking_active_resort f
GROUP BY bucket
ORDER BY bookings DESC;
