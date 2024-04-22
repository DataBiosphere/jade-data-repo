package bio.terra.service.snapshotbuilder.utils;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalToCompressingWhiteSpace;

import bio.terra.common.category.Unit;
import bio.terra.service.snapshotbuilder.query.SqlRenderContext;
import bio.terra.service.snapshotbuilder.query.SqlRenderContextTest;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

@Tag(Unit.TAG)
class HierarchyQueryBuilderTest {

  private static final String GCP_EXPECTED =
      """
      SELECT
          c.concept_id_1 AS parent_id, c.concept_id_2 AS concept_id, c0.concept_name, c0.concept_code,
             COUNT(DISTINCT c1.person_id) AS count,
             EXISTS (SELECT 1
           FROM concept_ancestor AS c2
                    JOIN concept AS c3 ON c3.concept_id = c2.descendant_concept_id
           WHERE (c2.ancestor_concept_id = c.concept_id_2 AND
                  c2.descendant_concept_id != c.concept_id_2 AND
                  c3.standard_concept = 'S'))
                 AS has_children
      FROM concept_relationship AS c
               JOIN concept AS c0 ON c0.concept_id = c.concept_id_2
               JOIN concept AS c4 ON c4.concept_id = c.concept_id_1
               LEFT JOIN condition_occurrence AS c1 ON c1.condition_concept_id = c.concept_id_2
      WHERE (c.concept_id_1 IN (SELECT c5.ancestor_concept_id
           FROM concept_ancestor AS c5
           WHERE (c5.descendant_concept_id = 1 AND c5.ancestor_concept_id != 1)) AND
             c.relationship_id = 'Subsumes' AND c4.standard_concept = 'S' AND c0.standard_concept = 'S')
      GROUP BY c0.concept_name, c.concept_id_1, c.concept_id_2, c0.concept_code
      ORDER BY c0.concept_name ASC""";

  private static final String AZURE_EXPECTED =
      """
      SELECT c.concept_id_1 AS parent_id, c.concept_id_2 AS concept_id, c0.concept_name, c0.concept_code,
        COUNT(DISTINCT c1.person_id) AS count,
        CASE
            WHEN EXISTS (SELECT 1
              FROM concept_ancestor AS c2
                       JOIN concept AS c3 ON c3.concept_id = c2.descendant_concept_id
              WHERE (c2.ancestor_concept_id = c.concept_id_2 AND
                     c2.descendant_concept_id != c.concept_id_2 AND
                     c3.standard_concept = 'S')) THEN 1
            ELSE 0 END AS has_children
      FROM concept_relationship AS c
         JOIN concept AS c0 ON c0.concept_id = c.concept_id_2
         JOIN concept AS c4 ON c4.concept_id = c.concept_id_1
         LEFT JOIN condition_occurrence AS c1 ON c1.condition_concept_id = c.concept_id_2
      WHERE (c.concept_id_1 IN (SELECT c5.ancestor_concept_id
               FROM concept_ancestor AS c5
               WHERE (c5.descendant_concept_id = 1 AND c5.ancestor_concept_id != 1)) AND
             c.relationship_id = 'Subsumes' AND c4.standard_concept = 'S' AND c0.standard_concept = 'S')
      GROUP BY c0.concept_name, c.concept_id_1, c.concept_id_2, c0.concept_code
      ORDER BY c0.concept_name ASC""";

  @ParameterizedTest
  @ArgumentsSource(SqlRenderContextTest.Contexts.class)
  void generateQuery(SqlRenderContext context) {
    var query =
        new HierarchyQueryBuilder()
            .generateQuery(ConceptChildrenQueryBuilderTest.createDomainOption(), 1);
    assertThat(
        query.renderSQL(context),
        equalToCompressingWhiteSpace(context.getPlatform().choose(GCP_EXPECTED, AZURE_EXPECTED)));
  }
}
