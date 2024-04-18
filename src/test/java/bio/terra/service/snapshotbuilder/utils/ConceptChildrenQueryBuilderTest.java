package bio.terra.service.snapshotbuilder.utils;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalToCompressingWhiteSpace;

import bio.terra.common.category.Unit;
import bio.terra.model.SnapshotBuilderDomainOption;
import bio.terra.service.snapshotbuilder.query.QueryTestUtils;
import bio.terra.service.snapshotbuilder.query.SqlRenderContext;
import bio.terra.service.snapshotbuilder.utils.constants.ConditionOccurrence;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

@Tag(Unit.TAG)
class ConceptChildrenQueryBuilderTest {

  private static final String GCP_EXPECTED =
      """
      SELECT c.concept_name, c.concept_id,
        COUNT(DISTINCT c0.person_id) AS count,
        EXISTS (SELECT 1
          FROM concept_ancestor AS c1
                   JOIN concept AS c2 ON c2.concept_id = c1.descendant_concept_id
          WHERE (c1.ancestor_concept_id = c.concept_id AND
                 c1.descendant_concept_id != c.concept_id AND c2.standard_concept = 'S'))
        AS has_children
      FROM concept AS c
        JOIN concept_ancestor AS c3 ON c3.ancestor_concept_id = c.concept_id
        JOIN condition_occurrence AS c0 ON c0.condition_concept_id = c3.descendant_concept_id
      WHERE (c.concept_id IN (SELECT c4.concept_id_2
          FROM concept_relationship AS c4
          WHERE (c4.concept_id_1 = 101 AND c4.relationship_id = 'Subsumes')) AND c.standard_concept = 'S')
      GROUP BY c.concept_name, c.concept_id
      ORDER BY c.concept_name ASC""";

  private static final String AZURE_EXPECTED =
      """
      SELECT c.concept_name, c.concept_id,
        COUNT(DISTINCT c0.person_id) AS count,
        CASE WHEN EXISTS (SELECT 1
          FROM concept_ancestor AS c1
                   JOIN concept AS c2 ON c2.concept_id = c1.descendant_concept_id
          WHERE (c1.ancestor_concept_id = c.concept_id AND
                 c1.descendant_concept_id != c.concept_id AND c2.standard_concept = 'S'))
        THEN 1 ELSE 0 END AS has_children
      FROM concept AS c
        JOIN concept_ancestor AS c3 ON c3.ancestor_concept_id = c.concept_id
        JOIN condition_occurrence AS c0 ON c0.condition_concept_id = c3.descendant_concept_id
      WHERE (c.concept_id IN (SELECT c4.concept_id_2
          FROM concept_relationship AS c4
          WHERE (c4.concept_id_1 = 101 AND c4.relationship_id = 'Subsumes')) AND c.standard_concept = 'S')
      GROUP BY c.concept_name, c.concept_id
      ORDER BY c.concept_name ASC""";

  private SnapshotBuilderDomainOption createDomainOption(
      String name, int id, String occurrenceTable, String columnName) {
    var option = new SnapshotBuilderDomainOption();
    option.name(name).id(id).tableName(occurrenceTable).columnName(columnName);
    return option;
  }

  @ParameterizedTest
  @ArgumentsSource(QueryTestUtils.Contexts.class)
  void buildConceptChildrenQuery(SqlRenderContext context) {
    SnapshotBuilderDomainOption domainOption =
        createDomainOption(
            "Condition",
            19,
            ConditionOccurrence.TABLE_NAME,
            ConditionOccurrence.CONDITION_CONCEPT_ID);
    String sql =
        new QueryBuilderFactory()
            .conceptChildrenQueryBuilder()
            .buildConceptChildrenQuery(domainOption, 101)
            .renderSQL(context);
    assertThat(
        sql,
        equalToCompressingWhiteSpace(context.getPlatform().choose(GCP_EXPECTED, AZURE_EXPECTED)));
  }

  @ParameterizedTest
  @ArgumentsSource(QueryTestUtils.Contexts.class)
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
    assertThat(sql, Matchers.equalToCompressingWhiteSpace(expected));
  }
}
