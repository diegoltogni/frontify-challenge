-- Fixing the dbt+scheema
USE database APPLICATION_TASKS;
USE schema DATA_ANALYTICS_ENGINEER;

/*
Write an SQL-statement that gets the
    - latest VALUE and the associated TIMESTAMP
    - per DEAL_ID and PROPERTY_NAME
    - from the HISTORIC_DEAL_PROPERTIES table.
*/


---------------------------
-- My answer:

SELECT deal_id
       , property_name
       , value
       , timestamp
  FROM historic_deal_properties
QUALIFY ROW_NUMBER() OVER (
        PARTITION BY deal_id
                     , property_name
            ORDER BY timestamp DESC
        ) = 1
;


---------------------------        
-- Double checking I didn't get anything wrong.

-- Checking a single combination of deal_id and property_name

SELECT *
  FROM historic_deal_properties
 WHERE deal_id = '17ad93e7ce6850ee24120138f9a07307eb18f6fd51f39158937f7ac5466931ec'
       AND property_name = 'num_notes'
 ORDER BY timestamp DESC
;
 

-- The query bellow should return NO rows

WITH most_recent_values AS (

    SELECT deal_id
           , property_name
           , MAX(timestamp) AS timestamp
      FROM historic_deal_properties
     GROUP BY 1,2

)
, my_answer AS (

    SELECT deal_id
           , property_name
           , value
           , timestamp
      FROM historic_deal_properties
   QUALIFY ROW_NUMBER() OVER (
            PARTITION BY deal_id
                         , property_name
                ORDER BY timestamp DESC
            ) = 1

)
SELECT *
  FROM most_recent_values a
  LEFT JOIN my_answer b ON a.deal_id=b.deal_id AND a.property_name=b.property_name
 WHERE a.timestamp != b.timestamp
;

