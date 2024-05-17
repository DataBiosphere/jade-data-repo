package bio.terra.service.snapshotbuilder.utils;

import static bio.terra.service.snapshotbuilder.utils.CriteriaQueryBuilderTest.assertQueryEquals;

import bio.terra.common.category.Unit;
import bio.terra.model.SnapshotBuilderDomainOption;
import bio.terra.service.snapshotbuilder.query.SqlRenderContext;
import bio.terra.service.snapshotbuilder.query.SqlRenderContextProvider;
import bio.terra.service.snapshotbuilder.utils.constants.ConditionOccurrence;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

@Tag(Unit.TAG)
class ConceptChildrenQueryBuilderTest {

  private static final String GCP_EXPECTED =
      """
      SELECT c.concept_name, c.concept_id, c.concept_code,
        COUNT(DISTINCT co.person_id) AS count,
        EXISTS (SELECT 1
          FROM concept_ancestor AS ca
                   JOIN concept AS c1 ON c1.concept_id = ca.descendant_concept_id
          WHERE (ca.ancestor_concept_id = c.concept_id AND
                 ca.descendant_concept_id != c.concept_id AND c1.standard_concept = 'S'))
        AS has_children
      FROM concept AS c
        JOIN concept_ancestor AS ca1 ON ca1.ancestor_concept_id = c.concept_id
        JOIN (SELECT concept_id_2 FROM concept_relationship AS cr
          WHERE (cr.concept_id_1 = 101 AND cr.relationship_id = 'Subsumes')) AS jf
          ON jf.concept_id_2 = c.concept_id
        LEFT JOIN condition_occurrence AS co ON co.condition_concept_id = ca1.descendant_concept_id
      WHERE c.standard_concept = 'S'
      GROUP BY c.concept_name, c.concept_id, c.concept_code
      ORDER BY c.concept_name ASC""";

  private static final String AZURE_EXPECTED =
      """
      SELECT c.concept_name, c.concept_id, c.concept_code,
        COUNT(DISTINCT co.person_id) AS count,
        CASE WHEN EXISTS (SELECT 1
          FROM concept_ancestor AS ca
                   JOIN concept AS c1 ON c1.concept_id = ca.descendant_concept_id
          WHERE (ca.ancestor_concept_id = c.concept_id AND
                 ca.descendant_concept_id != c.concept_id AND c1.standard_concept = 'S'))
        THEN 1 ELSE 0 END AS has_children
      FROM concept AS c
        JOIN concept_ancestor AS ca1 ON ca1.ancestor_concept_id = c.concept_id
        JOIN (SELECT concept_id_2 FROM concept_relationship AS cr
              WHERE (cr.concept_id_1 = 101 AND cr.relationship_id = 'Subsumes')) AS jf
              ON jf.concept_id_2 = c.concept_id
        LEFT JOIN condition_occurrence AS co ON co.condition_concept_id = ca1.descendant_concept_id
      WHERE c.standard_concept = 'S'
      GROUP BY c.concept_name, c.concept_id, c.concept_code
      ORDER BY c.concept_name ASC""";

  public static SnapshotBuilderDomainOption createDomainOption() {
    var option = new SnapshotBuilderDomainOption();
    option
        .name("Condition")
        .id(19)
        .tableName(ConditionOccurrence.TABLE_NAME)
        .columnName(ConditionOccurrence.CONDITION_CONCEPT_ID);
    return option;
  }

  @ParameterizedTest
  @ArgumentsSource(SqlRenderContextProvider.class)
  void buildConceptChildrenQuery(SqlRenderContext context) {
    String sql =
        new QueryBuilderFactory()
            .conceptChildrenQueryBuilder()
            .buildConceptChildrenQuery(createDomainOption(), 101)
            .renderSQL(context);

    assertQueryEquals(context.getPlatform().choose(GCP_EXPECTED, AZURE_EXPECTED), sql);
  }

  @ParameterizedTest
  @ArgumentsSource(SqlRenderContextProvider.class)
  void buildGetDomainIdQuery(SqlRenderContext context) {
    String sql =
        new QueryBuilderFactory()
            .conceptChildrenQueryBuilder()
            .retrieveDomainId(101)
            .renderSQL(context);
    String expected =
        """
        SELECT c.domain_id
        FROM concept AS c
        WHERE c.concept_id = 101""";
    assertQueryEquals(expected, sql);
  }
}
