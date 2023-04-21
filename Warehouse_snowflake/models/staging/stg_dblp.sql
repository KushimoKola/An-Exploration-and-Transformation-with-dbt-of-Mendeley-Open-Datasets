WITH flattened_dblp_data AS (
SELECT
    raw_data:"_id"."$oid"::STRING AS guid,
    raw_data:"_key"::STRING AS publication_key,
    REPLACE(REPLACE(REPLACE(raw_data:"author", '[', ''), ']', ''), '"', '') AS authors_name,
    raw_data:"title"::STRING AS title,
    raw_data:"booktitle"::STRING AS book_title,
    raw_data:"journal"::STRING AS journal,
    REPLACE(REPLACE(REPLACE(raw_data:"ee", '[', ''), ']', ''), '"', '') AS publication_ee,
    raw_data:"url"::STRING AS publication_url,
    raw_data:"mdate"::DATE AS created_at,
    raw_data:"month"::STRING AS publication_month,
    raw_data:"year"::STRING AS publication_year,
    raw_data:"number"::INT AS publication_number,
    raw_data:"pages"::STRING AS publication_pages,
    raw_data:"type"::STRING AS pub_type,
    raw_data:"volume"::STRING AS publication_volume,
    REPLACE(REPLACE(REPLACE(raw_data:"editor"::STRING, '[', ''), ']', ''), '"', '') AS editors_name,
    REPLACE(REPLACE(REPLACE(raw_data:"school", '[', ''), ']', ''), '"', '') AS school_name,
    REPLACE(REPLACE(REPLACE(raw_data:"crossref", '[', ''), ']', ''), '"', '') AS crossrefs,
    raw_data:"publisher"::STRING AS publisher,
    REPLACE(REPLACE(REPLACE(raw_data:"series", '[', ''), ']', ''), '"', '') AS series_name,
    raw_data:"cdrom"::STRING AS cdrom,
    REPLACE(REPLACE(REPLACE(raw_data:"note", '[', ''), ']', ''), '"', '') AS authors_note,
    REPLACE(REPLACE(REPLACE(raw_data:"isbn", '[', ''), ']', ''), '"', '') AS publication_isbn,
    raw_data:"cite" AS citations,
    raw_data:"publtype"::STRING AS publication_type
FROM
    {{ ref('source_dblp_data') }}
)
SELECT * from  flattened_dblp_data