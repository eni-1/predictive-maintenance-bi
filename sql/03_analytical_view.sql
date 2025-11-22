-- PROJECT: PREDICTIVE MAINTENANCE ANALYTICAL VIEW
-- Transforms raw telemetry into rolling features for BI

DROP VIEW IF EXISTS "ANALYTICAL_MAINTENANCE";
CREATE VIEW "ANALYTICAL_MAINTENANCE" AS

-- fix data types
WITH data_casting AS (
    SELECT
        "UDI" AS udi,
        "Product_ID" AS product_id,
        "Timestamp"::timestamp AS timestamp_clean,
        "Type" AS machine_type,
        "Machine_failure"::integer AS machine_failure,
        "Tool_wear" AS tool_wear,
        "Air_temperature" AS air_temperature,
        "Process_temperature" AS process_temperature,
        "Rotational_speed" AS rotational_speed,
        "Torque" AS torque
    FROM
        public.machine_data_raw
),

feature_engineering AS (
    SELECT
        *,
        -- f1: 1h rolling avg, range for gaps
        AVG(air_temperature) OVER (
            ORDER BY timestamp_clean
            RANGE BETWEEN INTERVAL '1 hour' PRECEDING AND CURRENT ROW
        ) AS avg_air_temp_1h,

        -- f2: rotational speed std, coalesce for nulls
        COALESCE(
            STDDEV(rotational_speed) OVER (
                ORDER BY timestamp_clean
                RANGE BETWEEN INTERVAL '1 hour' PRECEDING AND CURRENT ROW
            ), 0
        ) AS rot_speed_stability_1h,

        -- f3: rapid torque change
        torque - LAG(torque, 1) OVER (ORDER BY timestamp_clean) AS torque_change_instant,
        
        -- f4: efficiency
        process_temperature - air_temperature AS temp_differential
    FROM
        data_casting
)

SELECT
    *,
    -- risk tags
    CASE 
        WHEN machine_failure = 1 THEN 'Critical Failure'
        WHEN rot_speed_stability_1h > 20 THEN 'Unstable' -- logic test, can ignore
        WHEN tool_wear > 200 THEN 'High Wear Warning'
        ELSE 'Normal'
    END AS operational_status
FROM
    feature_engineering;