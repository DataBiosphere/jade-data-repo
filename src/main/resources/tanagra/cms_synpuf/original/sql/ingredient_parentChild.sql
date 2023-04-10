SELECT
  cr.concept_id_1 AS parent,
  cr.concept_id_2 AS child
FROM OPENROWSET(BULK 'parquet/concept_relationship/*/*.parquet', DATA_SOURCE = 'ds-ba420228-f13a-490a-93fc-fffd91e0f51b-pshapiro@test.firecloud.org', FORMAT='PARQUET') cr
       JOIN OPENROWSET(BULK 'parquet/concept/*/*.parquet', DATA_SOURCE = 'ds-ba420228-f13a-490a-93fc-fffd91e0f51b-pshapiro@test.firecloud.org', FORMAT='PARQUET') c1  ON c1.concept_id = cr.concept_id_1
       JOIN OPENROWSET(BULK 'parquet/concept/*/*.parquet', DATA_SOURCE = 'ds-ba420228-f13a-490a-93fc-fffd91e0f51b-pshapiro@test.firecloud.org', FORMAT='PARQUET') c2  ON c2.concept_id = cr.concept_id_2
WHERE
  cr.relationship_id IN ('Has form', 'RxNorm - ATC', 'RxNorm - ATC name', 'Mapped from', 'Subsumes')
  AND c1.concept_id != c2.concept_id

  AND ((c1.vocabulary_id IN ('RxNorm', 'RxNorm Extension')
          AND c1.concept_class_id = 'Ingredient'
          AND c1.standard_concept = 'S')
      OR (c1.vocabulary_id = 'RxNorm'
          AND c1.concept_class_id = 'Precise Ingredient')
      OR (c1.vocabulary_id = 'ATC'
          AND c1.concept_class_id IN ('ATC 1st', 'ATC 2nd', 'ATC 3rd', 'ATC 4th', 'ATC 5th')
          AND c1.standard_concept = 'C'))

  AND ((c2.vocabulary_id IN ('RxNorm', 'RxNorm Extension')
          AND c2.concept_class_id = 'Ingredient'
          AND c2.standard_concept = 'S')
      OR (c2.vocabulary_id = 'RxNorm'
          AND c2.concept_class_id = 'Precise Ingredient')
      OR (c2.vocabulary_id = 'ATC'
          AND c2.concept_class_id IN ('ATC 1st', 'ATC 2nd', 'ATC 3rd', 'ATC 4th', 'ATC 5th')
          AND c2.standard_concept = 'C'))

UNION ALL

SELECT
  ca.ancestor_concept_id AS parent,
  ca.descendant_concept_id AS child
FROM OPENROWSET(BULK 'parquet/concept_ancestor/*/*.parquet', DATA_SOURCE = 'ds-ba420228-f13a-490a-93fc-fffd91e0f51b-pshapiro@test.firecloud.org', FORMAT='PARQUET') ca
       JOIN OPENROWSET(BULK 'parquet/concept/*/*.parquet', DATA_SOURCE = 'ds-ba420228-f13a-490a-93fc-fffd91e0f51b-pshapiro@test.firecloud.org', FORMAT='PARQUET') c1  ON c1.concept_id = ca.concept_id_1
       JOIN OPENROWSET(BULK 'parquet/concept/*/*.parquet', DATA_SOURCE = 'ds-ba420228-f13a-490a-93fc-fffd91e0f51b-pshapiro@test.firecloud.org', FORMAT='PARQUET') c2  ON c2.concept_id = ca.concept_id_2
WHERE
  ca.min_levels_of_separation = 1
  AND ca.max_levels_of_separation = 1

  AND ((c1.vocabulary_id IN ('RxNorm', 'RxNorm Extension')
          AND c1.concept_class_id = 'Ingredient'
          AND c1.standard_concept = 'S')
      OR (c1.vocabulary_id = 'RxNorm'
          AND c1.concept_class_id = 'Precise Ingredient')
      OR (c1.vocabulary_id = 'ATC'
          AND c1.concept_class_id IN ('ATC 1st', 'ATC 2nd', 'ATC 3rd', 'ATC 4th', 'ATC 5th')
          AND c1.standard_concept = 'C'))

  AND ((c2.vocabulary_id IN ('RxNorm', 'RxNorm Extension')
          AND c2.concept_class_id = 'Ingredient'
          AND c2.standard_concept = 'S')
      OR (c2.vocabulary_id = 'RxNorm'
          AND c2.concept_class_id = 'Precise Ingredient')
      OR (c2.vocabulary_id = 'ATC'
          AND c2.concept_class_id IN ('ATC 1st', 'ATC 2nd', 'ATC 3rd', 'ATC 4th', 'ATC 5th')
          AND c2.standard_concept = 'C'))
