create or replace database weather_data;
create or replace schema weather;

-- All unqualified names (tables, tasks) resolve to the session's current database/schema.
-- If the task was created while you were in PUBLIC (or another schema), it inserts into
-- the wrong weather_hourly — so the pipe fills weather_raw_data but hourly stays empty.
USE DATABASE weather_data;
USE SCHEMA weather;
create or replace file format csv_weather_format
type = 'csv'
field_delimiter = ','
skip_header = 1
;
CREATE OR REPLACE STAGE s3_weather_data
    URL = 's3://weather-data-pipeline-v1/transformed_data/'
    CREDENTIALS = (AWS_KEY_ID='AKIA4ER33CQPY4AMXB77'           AWS_SECRET_KEY='k6iFAxtwBskPlBb+6M/PJaYcGo1qalL5t/TMSuNG')
    FILE_FORMAT = csv_weather_format;

LIST @s3_weather_data;
-- Load ALL existing files at once
--===============================================================
CREATE TABLE weather_data.weather.weather_raw_data (
    recorded_at     TIMESTAMP,
    latitude        FLOAT,
    longitude       FLOAT,
    timezone        STRING,
    temperature_f   FLOAT,
    precipitation_mm FLOAT,
    wind_speed_kmh  FLOAT,
    weather_code    INT,
    raw_data        VARIANT  -- store original JSON too
);

COPY INTO weather_data.weather.weather_raw_data (recorded_at, latitude, longitude, timezone, temperature_f, precipitation_mm, wind_speed_kmh, weather_code, raw_data)
FROM (
    SELECT $2::TIMESTAMP,
           $7::FLOAT,
           $8::FLOAT,
           $9::STRING,
           $3::FLOAT,
           $4::FLOAT,
           $5::FLOAT,
           $6::INT,
           OBJECT_CONSTRUCT(
               'recorded_at',      $2,
               'latitude',         $7,
               'longitude',        $8,
               'timezone',         $9,
               'elevation',        $10,
               'temperature_f',    $3,
               'precipitation_mm', $4,
               'wind_speed_kmh',   $5,
               'weather_code',     $6
           )
    FROM @s3_weather_data
)
FILE_FORMAT = csv_weather_format
ON_ERROR = 'CONTINUE';   -- skip bad files, don't stop the whole load
--================================================================================
CREATE OR REPLACE TABLE weather_data.weather.weather_hourly (
    recorded_at     TIMESTAMP,
    latitude        FLOAT,
    longitude       FLOAT,
    timezone        STRING,
    temperature_f   FLOAT,
    precipitation_mm FLOAT,
    wind_speed_kmh  FLOAT,
    weather_code    INT
    );

select *from weather_hourly;






select *FROM weather_raw_data;
 
CREATE OR REPLACE PIPE weather_pipe
    AUTO_INGEST = TRUE
AS
COPY INTO weather_data.weather.weather_raw_data (recorded_at, latitude, longitude, timezone, temperature_f, precipitation_mm, wind_speed_kmh, weather_code, raw_data)
FROM (
    SELECT
        $2::TIMESTAMP,
        $7::FLOAT,
        $8::FLOAT,
        $9::STRING,
        $3::FLOAT,
        $4::FLOAT,
        $5::FLOAT,
        $6::INT,
        OBJECT_CONSTRUCT(
            'recorded_at',      $2,
            'latitude',         $7,
            'longitude',        $8,
            'timezone',         $9,
            'elevation',        $10,
            'temperature_f',    $3,
            'precipitation_mm', $4,
            'wind_speed_kmh',   $5,
            'weather_code',     $6
        )
    FROM @s3_weather_data
)
FILE_FORMAT = csv_weather_format
;

-- Refresh to reprocess the 33 stuck files
ALTER PIPE weather_pipe REFRESH;





show pipes;

SELECT SYSTEM$PIPE_STATUS('weather_pipe');  -- check pipe status
show tasks;



-- Step 1: Drop and recreate the task with the correct SQL
CREATE OR REPLACE TASK transform_weather_data
    WAREHOUSE = COMPUTE_WH
    SCHEDULE  = 'USING CRON * * * * * UTC'
AS
INSERT INTO weather_data.weather.weather_hourly (
    recorded_at, latitude, longitude, timezone,
    temperature_f, precipitation_mm, wind_speed_kmh, weather_code
)
SELECT
    recorded_at,
    latitude,
    longitude,
    timezone,
    temperature_f,
    precipitation_mm,
    wind_speed_kmh,
    weather_code
FROM weather_data.weather.weather_raw_data
WHERE recorded_at > (
    SELECT COALESCE(MAX(recorded_at), '1900-01-01'::TIMESTAMP_NTZ)
    FROM weather_data.weather.weather_hourly
);

-- Step 2: Resume it (tasks start suspended after CREATE OR REPLACE)
ALTER TASK transform_weather_data RESUME;

-- Wait a few minutes, then check

SHOW TASKS;

-- select *from weather_hourly;    
-- show tasks;
-- show tables; 

-- ============================================================
-- SECTION 8: ANALYTICS VIEWS  (ready-made queries for BI tools)
-- ============================================================
 
-- Daily summary
CREATE OR REPLACE VIEW v_daily_summary AS
SELECT
    DATE(recorded_at)          AS weather_date,
    ROUND(AVG(temperature_f), 1)    AS avg_temp_f,
    ROUND(MIN(temperature_f), 1)    AS min_temp_f,
    ROUND(MAX(temperature_f), 1)    AS max_temp_f,
    ROUND(SUM(precipitation_mm), 2) AS total_precip_mm,
    ROUND(AVG(wind_speed_kmh), 1)   AS avg_wind_kmh,
    MODE(weather_code)         AS dominant_condition
FROM weather_data.weather.weather_hourly
GROUP BY DATE(recorded_at)
ORDER BY weather_date DESC;

select *from weather_raw_data;
SELECT *FROM weather_hourly;
SELECT
    NAME,
    STATE,
    SCHEDULED_TIME,
    QUERY_START_TIME,
    COMPLETED_TIME,
    NEXT_SCHEDULED_TIME,
    ERROR_MESSAGE
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    TASK_NAME => 'TRANSFORM_WEATHER_DATA',
    RESULT_LIMIT => 10
))
ORDER BY SCHEDULED_TIME DESC;