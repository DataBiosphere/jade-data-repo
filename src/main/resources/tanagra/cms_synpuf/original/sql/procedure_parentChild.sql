SELECT
  cr.concept_id_1 AS parent,
  cr.concept_id_2 AS child
FROM OPENROWSET(BULK 'parquet/concept_relationship/*/*.parquet', DATA_SOURCE = 'ds-ba420228-f13a-490a-93fc-fffd91e0f51b-pshapiro@test.firecloud.org', FORMAT='PARQUET') cr
       JOIN OPENROWSET(BULK 'parquet/concept/*/*.parquet', DATA_SOURCE = 'ds-ba420228-f13a-490a-93fc-fffd91e0f51b-pshapiro@test.firecloud.org', FORMAT='PARQUET') c1  ON c1.concept_id = cr.concept_id_1
       JOIN OPENROWSET(BULK 'parquet/concept/*/*.parquet', DATA_SOURCE = 'ds-ba420228-f13a-490a-93fc-fffd91e0f51b-pshapiro@test.firecloud.org', FORMAT='PARQUET') c2  ON c2.concept_id = cr.concept_id_2
WHERE
  cr.relationship_id = 'Subsumes'
  AND c1.domain_id = c2.domain_id
  AND c2.domain_id = 'Procedure'
  AND c1.vocabulary_id = c2.vocabulary_id
  AND c2.vocabulary_id = 'SNOMED'
