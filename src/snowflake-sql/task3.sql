-- Fixing the dbt+scheema
USE database APPLICATION_TASKS;
USE schema DATA_ANALYTICS_ENGINEER;

/*
Please use the following two tables for this task:
    - DEAL_DIM
    - DEAL_FACT
    
*/


---------------------------
/*
PS:
    There are some columns apparently missing from deal_fact:
        - DEAL_STAGE_OPPORTUNITY_DATE
        - OPPORTUNITY
        - DEAL_STAGE_BUSINESS_CASE_PROPOSAL_DATE
        - BUSINESS_CASE_PROPOSAL
    
    Closed Won = Booking
    
    There are some CURRENT_DEAL_STAGEs that don't map to any of the defined DEAL_STAGES:
        - Demo Booked
        - SQL on Hold
        - No Opportunity
        - Demo Completed
*/


---------------------------

-- a) EDA deal_dim
SELECT 'opportunity' AS stage
      , 1 AS stage_order
      , COUNT("OPPORTUNITY_DATE_(DEAL)") AS stage_count
      , MIN("OPPORTUNITY_DATE_(DEAL)") AS stage_min
      , MAX("OPPORTUNITY_DATE_(DEAL)") AS stage_max
 FROM deal_dim

UNION ALL

SELECT 'business_case_proposal' AS stage
       , 2 AS stage_order
       , COUNT("BUSINESS_CASE_PROPOSAL_DATE") AS stage_count
       , MIN("BUSINESS_CASE_PROPOSAL_DATE") AS stage_min
       , MAX("BUSINESS_CASE_PROPOSAL_DATE") AS stage_max
  FROM deal_dim

UNION ALL

SELECT 'negotiation' AS stage
       , 3 AS stage_order
       , COUNT("NEGOTIATION_DATE") AS stage_count
       , MIN("NEGOTIATION_DATE") AS stage_min
       , MAX("NEGOTIATION_DATE") AS stage_max
  FROM deal_dim

UNION ALL

SELECT 'verbal_agreement' AS stage
       , 4 AS stage_order
       , COUNT("VERBAL_AGREEMENT_DATE") AS stage_count
       , MIN("VERBAL_AGREEMENT_DATE") AS stage_min
       , MAX("VERBAL_AGREEMENT_DATE") AS stage_max
  FROM deal_dim

UNION ALL

SELECT 'booking' AS stage
       , 5 AS stage_order
       , COUNT("BOOKING_DATE") AS stage_count
       , MIN("BOOKING_DATE") AS stage_min
       , MAX("BOOKING_DATE") AS stage_max
  FROM deal_dim

UNION ALL

SELECT 'closed_lost' AS stage
       , 99 AS stage_order
       , COUNT("CLOSED_LOST_DATE") AS stage_count
       , MIN("CLOSED_LOST_DATE") AS stage_min
       , MAX("CLOSED_LOST_DATE") AS stage_max
  FROM deal_dim

;

-- a) EDA deal_fact
-- Checking the deal_stage flow (stage_n vs stage_n+1)
WITH defined_steps AS (

    SELECT *
           , CASE
                WHEN "CLOSED_LOST" = 1 THEN 'closed_lost'
                WHEN "BOOKING" = 1 THEN 'booking'
                WHEN "VERBAL_AGREEMENT" = 1 THEN 'verbal_agreement'
                WHEN "NEGOTIATION" = 1 THEN 'negotiation'
                WHEN "BUSINESS_CASE_PROPOSAL_AMOUNT" IS NOT NULL THEN 'business_case_proposal'
                ELSE 'opportunity' -- some are being missclassified? (to be investigated)
             END AS "DEAL_STAGE"
           , CASE
                WHEN "CLOSED_LOST" = 1 THEN 99
                WHEN "BOOKING" = 1 THEN 5
                WHEN "VERBAL_AGREEMENT" = 1 THEN 4
                WHEN "NEGOTIATION" = 1 THEN 3
                WHEN "BUSINESS_CASE_PROPOSAL_AMOUNT" IS NOT NULL THEN 2
                ELSE 1
             END AS "STAGE_ORDER"
      FROM deal_fact

)

, next_step AS (

    SELECT *
           , LAG("DEAL_STAGE", -1) OVER (PARTITION BY "DEAL_ID" ORDER BY "DEAL_STAGE_DATE" ASC) AS "NEXT_DEAL_STAGE"
           , LAG("DEAL_STAGE_DATE", -1) OVER (PARTITION BY "DEAL_ID" ORDER BY "DEAL_STAGE_DATE" ASC) AS "NEXT_DEAL_STAGE_DATE"
      FROM defined_steps

)

, time_span AS (

    SELECT "STAGE_ORDER"
           , "DEAL_STAGE"
           , "NEXT_DEAL_STAGE"
           , DATEDIFF('hour',"DEAL_STAGE_DATE","NEXT_DEAL_STAGE_DATE") AS "DEAL_STAGE_TIME_SPAN"
      FROM next_step

)

SELECT *
  FROM time_span
-- PIVOT (AVG("DEAL_STAGE_TIME_SPAN") FOR "NEXT_DEAL_STAGE" IN ('opportunity','business_case_proposal','negotiation','verbal_agreement','booking','closed_lost'))
 PIVOT (COUNT("DEAL_STAGE_TIME_SPAN") FOR "NEXT_DEAL_STAGE" IN ('opportunity'
                                                               ,'business_case_proposal'
                                                               ,'negotiation'
                                                               ,'verbal_agreement'
                                                               ,'booking'
                                                               ,'closed_lost' ))
  ORDER BY "STAGE_ORDER"
;

-----------

-- b) any discrepancies in the data which the business must adjust
-- Checking for deal_stage out of order
WITH defined_steps AS (

    SELECT *
           , CASE
                WHEN "CLOSED_LOST" = 1 THEN 'closed_lost'
                WHEN "BOOKING" = 1 THEN 'booking'
                WHEN "VERBAL_AGREEMENT" = 1 THEN 'verbal_agreement'
                WHEN "NEGOTIATION" = 1 THEN 'negotiation'
                WHEN "BUSINESS_CASE_PROPOSAL_AMOUNT" IS NOT NULL THEN 'business_case_proposal'
                ELSE 'opportunity'
             END AS "DEAL_STAGE"
           , CASE
                WHEN "CLOSED_LOST" = 1 THEN 99
                WHEN "BOOKING" = 1 THEN 5
                WHEN "VERBAL_AGREEMENT" = 1 THEN 4
                WHEN "NEGOTIATION" = 1 THEN 3
                WHEN "BUSINESS_CASE_PROPOSAL_AMOUNT" IS NOT NULL THEN 2
                ELSE 1
             END AS "STAGE_ORDER"
      FROM deal_fact

)

, previous_step AS (

    SELECT *
           , LAG("DEAL_STAGE", 1) OVER (PARTITION BY "DEAL_ID" ORDER BY "DEAL_STAGE_DATE" ASC) AS "PREVIOUS_DEAL_STAGE"
           , LAG("STAGE_ORDER", 1) OVER (PARTITION BY "DEAL_ID" ORDER BY "DEAL_STAGE_DATE" ASC) AS "PREVIOUS_STAGE_ORDER"
      FROM defined_steps

)

SELECT "PREVIOUS_DEAL_STAGE"
       , "PREVIOUS_STAGE_ORDER"
       , "DEAL_STAGE"
       , "STAGE_ORDER"
       , COUNT(1)
  FROM previous_step
 WHERE "PREVIOUS_STAGE_ORDER" >= "STAGE_ORDER"
 GROUP BY 1,2,3,4
 
---- Exclude values if they do not fit the deal pipeline definition
-- SELECT *
--   FROM previous_step
--  WHERE "PREVIOUS_STAGE_ORDER" < "STAGE_ORDER" OR "PREVIOUS_STAGE_ORDER" IS NULL

 ;

-- b) validate if the current_deal_stage is correctly defined
-- PS: some will be missclassified due to the unknown stages (eg. SQL on Hold)
WITH latest AS (

    SELECT *
           , CASE
                WHEN "CLOSED_LOST" = 1 THEN 'Closed Lost'
                WHEN "BOOKING" = 1 THEN 'Closed Won'
                WHEN "VERBAL_AGREEMENT" = 1 THEN 'Verbal Agreement'
                WHEN "NEGOTIATION" = 1 THEN 'Negotiation (P, L & S)'
                WHEN "BUSINESS_CASE_PROPOSAL_AMOUNT" IS NOT NULL THEN 'Business Case Proposal'
                ELSE 'Opportunity'
             END AS "CURRENT_DEAL_STATE_FACT"
      FROM deal_fact
   QUALIFY ROW_NUMBER() OVER (
            PARTITION BY "DEAL_ID"
                ORDER BY "DEAL_STAGE_DATE" DESC
            ) = 1

)

SELECT deal_dim."CURRENT_DEAL_STAGE"
       , latest."CURRENT_DEAL_STATE_FACT"
       , COUNT(1)
  FROM deal_dim
  LEFT JOIN latest ON latest."DEAL_ID" = deal_dim."DEAL_ID"
 WHERE deal_dim."CURRENT_DEAL_STAGE" != latest."CURRENT_DEAL_STATE_FACT"
 GROUP BY 1,2
;
