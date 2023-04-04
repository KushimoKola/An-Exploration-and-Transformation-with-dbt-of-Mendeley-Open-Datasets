CREATE TABLE REVENUE_AUDIT_FINAL AS (
WITH LEVEL_1 AS (
    SELECT
      user_id,
      regexp_matches(properties, '''(.*?)'': ''(.*?)''', 'g') AS parts
    FROM mytable
), 
LEVEL_2 AS (
  SELECT
    user_id,
    parts[1] AS property_key,
    parts[2] AS property_value
FROM LEVEL_1
),
normalized_prop as (
SELECT
  user_id,
  MAX(CASE WHEN property_key = 'gender' THEN property_value END) AS gender,
  MAX(CASE WHEN property_key = 'nationality' THEN property_value END) AS nationality,
  MAX(CASE WHEN property_key = 'document_type' THEN property_value END) AS document_type,
  MAX(CASE WHEN property_key = 'date_of_expiry' THEN property_value::date END) AS date_of_expiry,
  MAX(CASE WHEN property_key = 'issuing_country' THEN property_value END) AS issuing_country,
  MAX(CASE WHEN property_key = 'issuing_state' THEN property_value END) AS issuing_state,
  MAX(CASE WHEN property_key = 'issuing_date' THEN to_date(property_value, 'yyyy mm') END) AS issuing_date
FROM LEVEL_2
	GROUP BY 1
),
pro_scores as (
select
	user_id,
	regexp_replace(unnest(regexp_matches(properties_2, '\d+\.\d+')), '[{}]','', 'g')::float AS score
from mytable
),
joining_data as (
select a.*,
	score,
	gender,
	nationality,
	document_type,
	issuing_country,
	issuing_state,
	issuing_date as issuing_month,
	date_of_expiry
from mytable as a
	left join normalized_prop as b on a.user_id = b.user_id
	left join pro_scores as c on a.user_id = c.user_id
),
final as (
select
	"Id" as guid,
	user_id,
	"attempt_id",
	"attempt_id_2",
	properties,
	gender,
	nationality,
	document_type,
	issuing_country,
	issuing_state,
	issuing_month,
	date_of_expiry,
	properties_2,
	score,
	result,
	"result_2" as result_dup,
	"visual_authenticity_result",
	"visual_authenticity_result_2",
	"image_integrity_result",
	"face_detection_result",
	"image_quality_result",
	"supported_document_result",
	"conclusive_document_quality_result",
	"colour_picture_result",
	"data_validation_result",
	"data_consistency_result",
	"data_comparison_result",
	"police_record_result",
	"compromised_document_result",
	"sub_result",
	"face_comparison_result",
	"facial_image_integrity_result_2",
	"created_at",
	"created_2_at"
from joining_data
) 
SELECT * FROM FINAL
);