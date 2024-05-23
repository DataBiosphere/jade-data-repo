package bio.terra.service.snapshotbuilder.utils;

import static bio.terra.service.snapshotbuilder.utils.CriteriaQueryBuilderTest.assertQueryEquals;

import bio.terra.common.category.Unit;
import bio.terra.service.snapshotbuilder.query.SqlRenderContext;
import bio.terra.service.snapshotbuilder.query.SqlRenderContextProvider;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

@Tag(Unit.TAG)
class HierarchyQueryBuilderTest {

  private static final String GCP_EXPECTED =
      """
      SELECT
          cr.concept_id_1 AS parent_id, cr.concept_id_2 AS concept_id, c.concept_name, c.concept_code,
             COUNT(DISTINCT co.person_id) AS count,
             COUNT(DISTINCT hc.descendant_concept_id) > 0 AS has_children
        FROM concept_relationship AS cr
               JOIN concept AS c ON c.concept_id = cr.concept_id_2
               JOIN concept AS c1 ON c1.concept_id = cr.concept_id_1
               LEFT JOIN (SELECT ca.ancestor_concept_id, ca.descendant_concept_id, ca.min_levels_of_separation
                            FROM concept_ancestor AS ca
                            JOIN concept AS c2 ON c2.concept_id = ca.descendant_concept_id
                            WHERE c2.standard_concept = 'S') AS hc
                  ON (hc.ancestor_concept_id = cr.concept_id_2
                  AND hc.descendant_concept_id != cr.concept_id_2
                  AND hc.min_levels_of_separation <= 1)
               JOIN concept_ancestor AS ca1 ON ca1.ancestor_concept_id = cr.concept_id_2
               LEFT JOIN condition_occurrence AS co ON co.condition_concept_id = ca1.descendant_concept_id
               JOIN (SELECT ca2.ancestor_concept_id, ca2.descendant_concept_id
                      FROM concept_ancestor AS ca2
                      WHERE (ca2.descendant_concept_id = 1 AND
                      ca2.ancestor_concept_id != 1)) AS cas
                      ON cas.ancestor_concept_id = cr.concept_id_1
        WHERE (cr.relationship_id = 'Subsumes' AND c1.standard_concept = 'S' AND c.standard_concept = 'S')
        GROUP BY c.concept_name, cr.concept_id_1, cr.concept_id_2, c.concept_code
        ORDER BY c.concept_name ASC""";

  private static final String AZURE_EXPECTED =
      """
      SELECT
          cr.concept_id_1 AS parent_id, cr.concept_id_2 AS concept_id, c.concept_name, c.concept_code,
             COUNT(DISTINCT co.person_id) AS count,
             COUNT(DISTINCT hc.descendant_concept_id) AS has_children
        FROM concept_relationship AS cr
               JOIN concept AS c ON c.concept_id = cr.concept_id_2
               JOIN concept AS c1 ON c1.concept_id = cr.concept_id_1
               LEFT JOIN (SELECT ca.ancestor_concept_id, ca.descendant_concept_id, ca.min_levels_of_separation
                  FROM concept_ancestor AS ca
                  JOIN concept AS c2 ON c2.concept_id = ca.descendant_concept_id
                  WHERE c2.standard_concept = 'S') AS hc
                    ON (hc.ancestor_concept_id = cr.concept_id_2
                    AND hc.descendant_concept_id != cr.concept_id_2
                    AND hc.min_levels_of_separation <= 1)
               JOIN concept_ancestor AS ca1 ON ca1.ancestor_concept_id = cr.concept_id_2
               LEFT JOIN condition_occurrence AS co ON co.condition_concept_id = ca1.descendant_concept_id
               JOIN (SELECT ca2.ancestor_concept_id, ca2.descendant_concept_id
                    FROM concept_ancestor AS ca2
                      WHERE (ca2.descendant_concept_id = 1 AND
                      ca2.ancestor_concept_id != 1)) AS cas
                      ON cas.ancestor_concept_id = cr.concept_id_1
        WHERE (cr.relationship_id = 'Subsumes' AND c1.standard_concept = 'S' AND c.standard_concept = 'S')
        GROUP BY c.concept_name, cr.concept_id_1, cr.concept_id_2, c.concept_code
        ORDER BY c.concept_name ASC""";

  @ParameterizedTest
  @ArgumentsSource(SqlRenderContextProvider.class)
  void generateQuery(SqlRenderContext context) {
    var query =
        new HierarchyQueryBuilder()
            .generateQuery(ConceptChildrenQueryBuilderTest.createDomainOption(), 1);
    assertQueryEquals(
        context.getPlatform().choose(GCP_EXPECTED, AZURE_EXPECTED), query.renderSQL(context));
  }
}
