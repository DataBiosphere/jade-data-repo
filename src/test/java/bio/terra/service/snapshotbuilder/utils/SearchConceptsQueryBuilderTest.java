package bio.terra.service.snapshotbuilder.utils;

import static bio.terra.service.snapshotbuilder.utils.SearchConceptsQueryBuilder.createSearchConceptClause;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalToCompressingWhiteSpace;
import static org.hamcrest.Matchers.is;

import bio.terra.common.CloudPlatformWrapper;
import bio.terra.common.category.Unit;
import bio.terra.model.SnapshotBuilderDomainOption;
import bio.terra.service.snapshotbuilder.query.QueryTestUtils;
import bio.terra.service.snapshotbuilder.query.SqlRenderContext;
import bio.terra.service.snapshotbuilder.query.TablePointer;
import bio.terra.service.snapshotbuilder.query.TableVariable;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@Tag(Unit.TAG)
class SearchConceptsQueryBuilderTest {

  private SnapshotBuilderDomainOption createDomainOption(
      String name, int id, String occurrenceTable, String columnName) {
    var option = new SnapshotBuilderDomainOption();
    option.name(name).id(id).tableName(occurrenceTable).columnName(columnName);
    return option;
  }

  @ParameterizedTest
  @ArgumentsSource(QueryTestUtils.Contexts.class)
  void buildSearchConceptsQuery(SqlRenderContext context) {
    CloudPlatformWrapper platformWrapper = context.getPlatform();
    SnapshotBuilderDomainOption domainOption =
        createDomainOption("Observation", 27, "observation", "observation_concept_id");
    String actual =
        new QueryBuilderFactory()
            .searchConceptsQueryBuilder(x -> x)
            .buildSearchConceptsQuery(domainOption, "cancer")
            .renderSQL(platformWrapper);

    assertThat(
        "generated SQL for GCP is correct",
        actual,
        equalToCompressingWhiteSpace(
            platformWrapper.choose(
                () ->
                    "SELECT c.concept_name, c.concept_id, COUNT(DISTINCT o.person_id) AS count "
                        + "FROM concept AS c "
                        + "JOIN concept_ancestor AS c0 ON c0.ancestor_concept_id = c.concept_id "
                        + "LEFT JOIN observation AS o ON o.observation_concept_id = c0.descendant_concept_id "
                        + "WHERE (c.domain_id = 'Observation' "
                        + "AND (CONTAINS_SUBSTR(c.concept_name, 'cancer') "
                        + "OR CONTAINS_SUBSTR(c.concept_code, 'cancer'))) "
                        + "GROUP BY c.concept_name, c.concept_id "
                        + "ORDER BY count DESC "
                        + "LIMIT 100",
                () ->
                    "SELECT TOP 100 c.concept_name, c.concept_id, COUNT(DISTINCT o.person_id) AS count "
                        + "FROM concept AS c  "
                        + "JOIN concept_ancestor AS c0 ON c0.ancestor_concept_id = c.concept_id "
                        + "LEFT JOIN observation AS o ON o.observation_concept_id = c0.descendant_concept_id "
                        + "WHERE (c.domain_id = 'Observation' "
                        + "AND (CHARINDEX('cancer', c.concept_name) > 0 "
                        + "OR CHARINDEX('cancer', c.concept_code) > 0)) "
                        + "GROUP BY c.concept_name, c.concept_id "
                        + "ORDER BY count DESC")));
  }
  ;

  @ParameterizedTest
  @ArgumentsSource(QueryTestUtils.Contexts.class)
  void buildSearchConceptsQueryEmpty(SqlRenderContext context) {
    CloudPlatformWrapper platformWrapper = context.getPlatform();
    SnapshotBuilderDomainOption domainOption =
        createDomainOption("Condition", 19, "condition_occurrence", "condition_concept_id");
    String actual =
        new QueryBuilderFactory()
            .searchConceptsQueryBuilder(x -> x)
            .buildSearchConceptsQuery(domainOption, "")
            .renderSQL(platformWrapper);
    String expected =
        "c.concept_name, c.concept_id, COUNT(DISTINCT c1.person_id) AS count "
            + "FROM concept AS c  "
            + "JOIN concept_ancestor AS c0 ON c0.ancestor_concept_id = c.concept_id "
            + "LEFT JOIN condition_occurrence AS c1 ON c1.condition_concept_id = c0.descendant_concept_id "
            + "WHERE c.domain_id = 'Condition' "
            + "GROUP BY c.concept_name, c.concept_id "
            + "ORDER BY count DESC";

    assertThat(
        "generated SQL for GCP and Azure empty search string is correct",
        actual,
        // table name is added when the Query is created
        equalToCompressingWhiteSpace(
            platformWrapper.choose(
                () -> "SELECT " + expected + " LIMIT 100", () -> "SELECT TOP 100 " + expected)));
  }

  @ParameterizedTest
  @ArgumentsSource(QueryTestUtils.Contexts.class)
  void testCreateSearchConceptClause(SqlRenderContext context) {
    CloudPlatformWrapper platformWrapper = context.getPlatform();
    TablePointer conceptTablePointer = TablePointer.fromTableName("concept");
    TableVariable conceptTableVariable = TableVariable.forPrimary(conceptTablePointer);
    String actual =
        createSearchConceptClause(
                conceptTablePointer, conceptTableVariable, "cancer", "concept_name")
            .renderSQL(CloudPlatformWrapper.of(platform));

    assertThat(
        "generated sql is as expected",
        actual,
        // table name is added when the Query is created
        equalToCompressingWhiteSpace(
            platformWrapper.choose(
                () -> "CONTAINS_SUBSTR(null.concept_name, 'cancer')",
                () -> "CHARINDEX('cancer', null.concept_name) > 0")));

  }

  @ParameterizedTest
  @ArgumentsSource(QueryTestUtils.Contexts.class)
  void testCreateDomainClause(SqlRenderContext context) {
    TablePointer conceptTablePointer = TablePointer.fromTableName("concept");
    TableVariable conceptTableVariable = TableVariable.forPrimary(conceptTablePointer);

    assertThat(
        "generated sql is as expected",
        SearchConceptsQueryBuilder.createDomainClause(
                conceptTablePointer, conceptTableVariable, "cancer")
            .renderSQL(context),
        is("c.domain_id = 'cancer'"));
  }
}
