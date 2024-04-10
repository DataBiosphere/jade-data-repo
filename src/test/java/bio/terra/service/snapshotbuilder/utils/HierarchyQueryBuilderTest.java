package bio.terra.service.snapshotbuilder.utils;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalToCompressingWhiteSpace;

import bio.terra.common.category.Unit;
import bio.terra.service.snapshotbuilder.query.QueryTestUtils;
import bio.terra.service.snapshotbuilder.query.SqlRenderContext;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

@Tag(Unit.TAG)
class HierarchyQueryBuilderTest {

  private static final String GCP_EXPECTED =
      """
      SELECT
        c.concept_id_1 AS parent_id, c.concept_id_2 AS concept_id, c0.concept_name, c0.concept_code,
        EXISTS (SELECT 1
          FROM concept_ancestor AS c1
                   JOIN concept AS c2 ON c2.concept_id = c1.descendant_concept_id
          WHERE (c1.ancestor_concept_id = c0.concept_id AND
                 c1.descendant_concept_id != c0.concept_id AND
                 c2.standard_concept = 'S'))
          AS has_children
      FROM concept_relationship AS c
       JOIN concept AS c0 ON c0.concept_id = c.concept_id_2
       JOIN concept AS c3 ON c3.concept_id = c.concept_id_1
      WHERE (c.concept_id_1 IN (SELECT c4.ancestor_concept_id
              FROM concept_ancestor AS c4
              WHERE (c4.descendant_concept_id = 1 AND c4.ancestor_concept_id != 1)) AND
          c.relationship_id = 'Subsumes' AND c3.standard_concept = 'S' AND c0.standard_concept = 'S')""";

  private static final String AZURE_EXPECTED =
      """
      SELECT
        c.concept_id_1 AS parent_id, c.concept_id_2 AS concept_id, c0.concept_name, c0.concept_code,
        CASE
            WHEN EXISTS (SELECT 1
              FROM concept_ancestor AS c1
                       JOIN concept AS c2 ON c2.concept_id = c1.descendant_concept_id
              WHERE (c1.ancestor_concept_id = c0.concept_id AND
                     c1.descendant_concept_id != c0.concept_id AND
                     c2.standard_concept = 'S')) THEN 1
            ELSE 0 END AS has_children
      FROM concept_relationship AS c
       JOIN concept AS c0 ON c0.concept_id = c.concept_id_2
       JOIN concept AS c3 ON c3.concept_id = c.concept_id_1
      WHERE (c.concept_id_1 IN (SELECT c4.ancestor_concept_id
              FROM concept_ancestor AS c4
              WHERE (c4.descendant_concept_id = 1 AND c4.ancestor_concept_id != 1)) AND
          c.relationship_id = 'Subsumes' AND c3.standard_concept = 'S' AND c0.standard_concept = 'S')""";

  @ParameterizedTest
  @ArgumentsSource(QueryTestUtils.Contexts.class)
  void generateQuery(SqlRenderContext context) {
    var query = new HierarchyQueryBuilder().generateQuery(1);
    assertThat(
        query.renderSQL(context),
        equalToCompressingWhiteSpace(context.getPlatform().choose(GCP_EXPECTED, AZURE_EXPECTED)));
  }
}
