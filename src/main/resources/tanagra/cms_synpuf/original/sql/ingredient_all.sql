SELECT
  c.concept_id AS id, c.concept_name AS name, c.vocabulary_id, c.standard_concept, c.concept_code
FROM OPENROWSET(BULK 'parquet/concept/*/*.parquet', DATA_SOURCE = 'ds-ba420228-f13a-490a-93fc-fffd91e0f51b-pshapiro@test.firecloud.org', FORMAT='PARQUET') c
WHERE c.domain_id = 'Drug'
AND ((c.vocabulary_id IN ('RxNorm', 'RxNorm Extension')
        AND c.concept_class_id = 'Ingredient'
        AND c.standard_concept = 'S')
    OR (c.vocabulary_id = 'RxNorm'
        AND c.concept_class_id = 'Precise Ingredient')
    OR (c.vocabulary_id = 'ATC'
        AND c.concept_class_id IN ('ATC 1st', 'ATC 2nd', 'ATC 3rd', 'ATC 4th', 'ATC 5th')
        AND c.standard_concept = 'C'))
