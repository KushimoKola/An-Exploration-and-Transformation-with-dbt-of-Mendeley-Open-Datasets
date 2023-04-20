WITH flattened_dblp_data AS (
SELECT
    raw_data:"_id"."$oid" AS id,
    raw_data:"_key" AS key,
    raw_data:"author" AS author,
    --author.value AS author,
    --raw_data:"author"[0] AS author1,
    --raw_data:"author"[1] AS author2,
    --raw_data:"author"[2] AS author3,
    raw_data:"title" AS title,
    raw_data:"booktitle" AS book_title,
    raw_data:"journal" AS journal,
    raw_data:"ee" AS ee,
    raw_data:"url" AS url,
    raw_data:"mdate" AS mdate,
    raw_data:"month" AS month,
    raw_data:"year" AS year,
    raw_data:"number" AS number,
    raw_data:"pages" AS pages,
    raw_data:"type" AS type,
    raw_data:"volume" AS volume,
    raw_data:"editor" AS editor,
    raw_data:"school" AS school,
    raw_data:"crossref" AS crossref,
    raw_data:"publisher" AS publisher,
    raw_data:"series" AS series,
    raw_data:"cdrom" AS cdrom,
    raw_data:"note" AS note,
    raw_data:"isbn" AS isbn,
    raw_data:"cite" AS citations,
    raw_data:"publtype" AS publication_type
FROM
    {{ ref('source_dblp_data') }}
)
SELECT * from  flattened_dblp_data