drop table if exists output_claim_edit_report_final;

create table output_claim_edit_report_final as 
with output_claim_edit_report_final  as(
with edits_with_reference as (
select a.cs_serial_number,
       concat(claim_id, '-', sl) as action_flattened_claim_id, 
       a.claim_id, 
       a.sl as claim_line_number, 
       a.savings as actual_savings_based_on_allowed_amount, 
       a.rule_id as concept_id,
       NULL as validator_recommendation, 
       a.recommended_payment_percentage as recommended_payment_percentage, 
       NULL as exceeded_units, 
       NULL as reference_flattened_claim_id, 
       a.status as concept_action, 
       NULL as payer_payment_status, 
        a.carc, 
       a.carc_message as carc_description,
       NULL as concept_run_order
from public.cs_claim_edit_results a  where job_id = 'staging-1HR85NAM1E71U'
), RankedValues AS (
        SELECT
            B.action_flattened_claim_id,
            B.concept_id,
            B.reference_flattened_claim_id,
            ROW_NUMBER() OVER ( 
                PARTITION BY B.action_flattened_claim_id, B.concept_id
                ORDER BY SPLIT_PART(B.reference_flattened_claim_id, '-', 1),SPLIT_PART(B.reference_flattened_claim_id, '-', 2)
            ) AS rn
        FROM edits_with_reference B
    )
    SELECT
        A.cs_serial_number,
        A.action_flattened_claim_id,
        A.claim_id as action_claim_id, 
        A.claim_line_number as action_claim_line,
        cast(A.actual_savings_based_on_allowed_amount as decimal(20,2)) as savings_opportunity,
        A.concept_id,
        A.concept_action as action,
        A.carc as carc_code,
        A.carc_description as carc_code_description,
        A.validator_recommendation,
        A.recommended_payment_percentage,
        A.exceeded_units,
        A.payer_payment_status,
        A.concept_run_order,
        MAX(CASE WHEN rv.rn = 1 THEN rv.reference_flattened_claim_id END) AS reference_flattened_claim_id_1,
        MAX(CASE WHEN rv.rn = 2 THEN rv.reference_flattened_claim_id END) AS reference_flattened_claim_id_2,
        MAX(CASE WHEN rv.rn = 3 THEN rv.reference_flattened_claim_id END) AS reference_flattened_claim_id_3,
        MAX(CASE WHEN rv.rn = 4 THEN rv.reference_flattened_claim_id END) AS reference_flattened_claim_id_4,
        MAX(CASE WHEN rv.rn = 5 THEN rv.reference_flattened_claim_id END) AS reference_flattened_claim_id_5,
        MAX(CASE WHEN rv.rn = 6 THEN rv.reference_flattened_claim_id END) AS reference_flattened_claim_id_6,
        MAX(CASE WHEN rv.rn = 7 THEN rv.reference_flattened_claim_id END) AS reference_flattened_claim_id_7,
        MAX(CASE WHEN rv.rn = 8 THEN rv.reference_flattened_claim_id END) AS reference_flattened_claim_id_8,
        MAX(CASE WHEN rv.rn = 9 THEN rv.reference_flattened_claim_id END) AS reference_flattened_claim_id_9,
        MAX(CASE WHEN rv.rn = 10 THEN rv.reference_flattened_claim_id END) AS reference_flattened_claim_id_10

    FROM (
        SELECT action_flattened_claim_id, concept_id, claim_id, claim_line_number, actual_savings_based_on_allowed_amount, exceeded_units, validator_recommendation, recommended_payment_percentage, cs_serial_number, concept_action,payer_payment_status, carc,carc_description,concept_run_order
        FROM edits_with_reference
        GROUP BY action_flattened_claim_id, concept_id, claim_id, claim_line_number, actual_savings_based_on_allowed_amount, exceeded_units, validator_recommendation, recommended_payment_percentage, cs_serial_number, concept_action,payer_payment_status,carc,carc_description,concept_run_order
    ) AS A
    LEFT JOIN RankedValues rv
        ON A.action_flattened_claim_id = rv.action_flattened_claim_id
        and A.concept_id = rv.concept_id
    GROUP BY
        A.cs_serial_number,
        A.action_flattened_claim_id,
        A.claim_id,
        A.claim_line_number,
        A.actual_savings_based_on_allowed_amount,
        A.concept_id,
        A.concept_action,
        A.carc,
        A.carc_description,
        A.validator_recommendation,
        A.recommended_payment_percentage,
        A.exceeded_units,
        A.payer_payment_status,
        A.concept_run_order
    ORDER BY
        A.action_flattened_claim_id
)
select * from output_claim_edit_report_final;






drop table if exists output_claims_edit_report_submission;
create table output_claims_edit_report_submission as 
With output_claims_edit_report_submission as (
with multiple_edits as (
  select a.cs_serial_number,
   a.action_flattened_claim_id ,
   a.action_claim_id, 
   a.action_claim_line, 
   a.concept_id, 
   a.action, 
   a.carc_code, 
   a.carc_code_description, 
   a.reference_flattened_claim_id_1, 
   a.reference_flattened_claim_id_2, 
   a.reference_flattened_claim_id_3, 
   a.reference_flattened_claim_id_4, 
   a.reference_flattened_claim_id_5, 
   a.reference_flattened_claim_id_6, 
   a.reference_flattened_claim_id_7, 
   a.reference_flattened_claim_id_8, 
   a.reference_flattened_claim_id_9, 
   a.reference_flattened_claim_id_10,
   a.validator_recommendation, 
   a.recommended_payment_percentage, 
   a.exceeded_units, case when a.savings_opportunity > 0 then a.savings_opportunity else 0 end as savings_opportunity,
  row_number() over (partition by action_flattened_claim_id 
    order by a.concept_run_order asc
    ) as rn
  from output_claim_edit_report_final a
  -- left join cs_he_production_databricks.silver.il_submission_final_concept_list_01062026_updated b 
  -- on a.concept_id = b.concept_id
) select 
  cs_serial_number,
  action_flattened_claim_id,
  action_claim_id,
  action_claim_line,
  max(case when me.rn = 1 then me.concept_id end) as concept_id1,
  max(case when me.rn = 1 then me.action end) as concept_id1_action,
  max(case when me.rn = 1 then me.carc_code end) as concept_id1_carc_code,
  max(case when me.rn = 1 then me.carc_code_description end) as concept_id1_carc_code_description,
  max(case when me.rn = 1 then me.recommended_payment_percentage end) as concept_id1_recommended_payment_percentage,
  max(case when me.rn = 1 then me.exceeded_units end) as concept_id1_exceeded_units,
  max(case when me.rn = 1 then me.savings_opportunity end) as concept_id1_savings_opportunity,
  max(case when me.rn = 1 then me.reference_flattened_claim_id_1 end) as concept_id1_reference_flattened_claim_id_1,
  max(case when me.rn = 1 then me.reference_flattened_claim_id_2 end) as concept_id1_reference_flattened_claim_id_2,
  max(case when me.rn = 1 then me.reference_flattened_claim_id_3 end) as concept_id1_reference_flattened_claim_id_3,
  max(case when me.rn = 1 then me.reference_flattened_claim_id_4 end) as concept_id1_reference_flattened_claim_id_4,
  max(case when me.rn = 1 then me.reference_flattened_claim_id_5 end) as concept_id1_reference_flattened_claim_id_5,
  max(case when me.rn = 1 then me.reference_flattened_claim_id_6 end) as concept_id1_reference_flattened_claim_id_6,
  max(case when me.rn = 1 then me.reference_flattened_claim_id_7 end) as concept_id1_reference_flattened_claim_id_7,
  max(case when me.rn = 1 then me.reference_flattened_claim_id_8 end) as concept_id1_reference_flattened_claim_id_8,
  max(case when me.rn = 1 then me.reference_flattened_claim_id_9 end) as concept_id1_reference_flattened_claim_id_9,
  max(case when me.rn = 1 then me.reference_flattened_claim_id_10 end) as concept_id1_reference_flattened_claim_id_10,
  max(case when me.rn = 1 then me.validator_recommendation end) as concept_id1_validator_recommendation,
  -- 2nd concept
  max(case when me.rn = 2 then me.concept_id end) as concept_id2,
  max(case when me.rn = 2 then me.action end) as concept_id2_action,
  max(case when me.rn = 2 then me.carc_code end) as concept_id2_carc_code,
  max(case when me.rn = 2 then me.carc_code_description end) as concept_id2_carc_code_description,
  max(case when me.rn = 2 then me.recommended_payment_percentage end) as concept_id2_recommended_payment_percentage,
  max(case when me.rn = 2 then me.exceeded_units end) as concept_id2_exceeded_units,
  max(case when me.rn = 2 then me.savings_opportunity end) as concept_id2_savings_opportunity,
  max(case when me.rn = 2 then me.reference_flattened_claim_id_1 end) as concept_id2_reference_flattened_claim_id_1,
  max(case when me.rn = 2 then me.reference_flattened_claim_id_2 end) as concept_id2_reference_flattened_claim_id_2,
  max(case when me.rn = 2 then me.reference_flattened_claim_id_3 end) as concept_id2_reference_flattened_claim_id_3,
  max(case when me.rn = 2 then me.reference_flattened_claim_id_4 end) as concept_id2_reference_flattened_claim_id_4,
  max(case when me.rn = 2 then me.reference_flattened_claim_id_5 end) as concept_id2_reference_flattened_claim_id_5,
  max(case when me.rn = 2 then me.reference_flattened_claim_id_6 end) as concept_id2_reference_flattened_claim_id_6,
  max(case when me.rn = 2 then me.reference_flattened_claim_id_7 end) as concept_id2_reference_flattened_claim_id_7,
  max(case when me.rn = 2 then me.reference_flattened_claim_id_8 end) as concept_id2_reference_flattened_claim_id_8,
  max(case when me.rn = 2 then me.reference_flattened_claim_id_9 end) as concept_id2_reference_flattened_claim_id_9,
  max(case when me.rn = 2 then me.reference_flattened_claim_id_10 end) as concept_id2_reference_flattened_claim_id_10,
  max(case when me.rn = 2 then me.validator_recommendation end) as concept_id2_validator_recommendation,
  -- 3rd concept
  max(case when me.rn = 3 then me.concept_id end) as concept_id3,
  max(case when me.rn = 3 then me.action end) as concept_id3_action,
  max(case when me.rn = 3 then me.carc_code end) as concept_id3_carc_code,
  max(case when me.rn = 3 then me.carc_code_description end) as concept_id3_carc_code_description,
  max(case when me.rn = 3 then me.recommended_payment_percentage end) as concept_id3_recommended_payment_percentage,
  max(case when me.rn = 3 then me.exceeded_units end) as concept_id3_exceeded_units,
  max(case when me.rn = 3 then me.savings_opportunity end) as concept_id3_savings_opportunity,
  max(case when me.rn = 3 then me.reference_flattened_claim_id_1 end) as concept_id3_reference_flattened_claim_id_1,
  max(case when me.rn = 3 then me.reference_flattened_claim_id_2 end) as concept_id3_reference_flattened_claim_id_2,
  max(case when me.rn = 3 then me.reference_flattened_claim_id_3 end) as concept_id3_reference_flattened_claim_id_3,
  max(case when me.rn = 3 then me.reference_flattened_claim_id_4 end) as concept_id3_reference_flattened_claim_id_4,
  max(case when me.rn = 3 then me.reference_flattened_claim_id_5 end) as concept_id3_reference_flattened_claim_id_5,
  max(case when me.rn = 3 then me.reference_flattened_claim_id_6 end) as concept_id3_reference_flattened_claim_id_6,
  max(case when me.rn = 3 then me.reference_flattened_claim_id_7 end) as concept_id3_reference_flattened_claim_id_7,
  max(case when me.rn = 3 then me.reference_flattened_claim_id_8 end) as concept_id3_reference_flattened_claim_id_8,
  max(case when me.rn = 3 then me.reference_flattened_claim_id_9 end) as concept_id3_reference_flattened_claim_id_9,
  max(case when me.rn = 3 then me.reference_flattened_claim_id_10 end) as concept_id3_reference_flattened_claim_id_10,
  max(case when me.rn = 3 then me.validator_recommendation end) as concept_id3_validator_recommendation,
  -- 4th concept
  max(case when me.rn = 4 then me.concept_id end) as concept_id4,
  max(case when me.rn = 4 then me.action end) as concept_id4_action,
  max(case when me.rn = 4 then me.carc_code end) as concept_id4_carc_code,
  max(case when me.rn = 4 then me.carc_code_description end) as concept_id4_carc_code_description,
  max(case when me.rn = 4 then me.recommended_payment_percentage end) as concept_id4_recommended_payment_percentage,
  max(case when me.rn = 4 then me.exceeded_units end) as concept_id4_exceeded_units,
  max(case when me.rn = 4 then me.savings_opportunity end) as concept_id4_savings_opportunity,
  max(case when me.rn = 4 then me.reference_flattened_claim_id_1 end) as concept_id4_reference_flattened_claim_id_1,
  max(case when me.rn = 4 then me.reference_flattened_claim_id_2 end) as concept_id4_reference_flattened_claim_id_2,
  max(case when me.rn = 4 then me.reference_flattened_claim_id_3 end) as concept_id4_reference_flattened_claim_id_3,
  max(case when me.rn = 4 then me.reference_flattened_claim_id_4 end) as concept_id4_reference_flattened_claim_id_4,
  max(case when me.rn = 4 then me.reference_flattened_claim_id_5 end) as concept_id4_reference_flattened_claim_id_5,
  max(case when me.rn = 4 then me.reference_flattened_claim_id_6 end) as concept_id4_reference_flattened_claim_id_6,
  max(case when me.rn = 4 then me.reference_flattened_claim_id_7 end) as concept_id4_reference_flattened_claim_id_7,
  max(case when me.rn = 4 then me.reference_flattened_claim_id_8 end) as concept_id4_reference_flattened_claim_id_8,
  max(case when me.rn = 4 then me.reference_flattened_claim_id_9 end) as concept_id4_reference_flattened_claim_id_9,
  max(case when me.rn = 4 then me.reference_flattened_claim_id_10 end) as concept_id4_reference_flattened_claim_id_10,
  max(case when me.rn = 4 then me.validator_recommendation end) as concept_id4_validator_recommendation
 from multiple_edits me
 group by 
  me.cs_serial_number,
  me.action_flattened_claim_id,
  me.action_claim_id,
  me.action_claim_line
  )
select * from output_claims_edit_report_submission;


select * from output_claims_edit_report_submission;




