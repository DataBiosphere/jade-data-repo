SELECT
  c.concept_id AS id, c.concept_name AS name, c.vocabulary_id, c.standard_concept, c.concept_code
FROM  `bigquery-public-data.cms_synthetic_patient_data_omop.concept` c
WHERE c.domain_id = 'Drug'
AND ((c.vocabulary_id IN ('RxNorm', 'RxNorm Extension')
        AND c.concept_class_id = 'Ingredient'
        AND c.standard_concept = 'S')
    OR (c.vocabulary_id = 'RxNorm'
        AND c.concept_class_id = 'Precise Ingredient')
    OR (c.vocabulary_id = 'ATC'
        AND c.concept_class_id IN ('ATC 1st', 'ATC 2nd', 'ATC 3rd', 'ATC 4th', 'ATC 5th')
        AND c.standard_concept = 'C'))