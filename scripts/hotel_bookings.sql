/* ============================================================
   HOTEL_BOOKINGS (PSA → DWH → DM)
   ============================================================ */

/* ------------------------------------------------------------
   STEP 1 (PSA): Create database & select schema
   ------------------------------------------------------------ */
DROP DATABASE IF EXISTS hotel_bookings;
CREATE DATABASE hotel_bookings;
USE hotel_bookings;

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
