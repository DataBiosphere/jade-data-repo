package bio.terra.service.snapshotbuilder.utils;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalToCompressingWhiteSpace;

import bio.terra.common.CloudPlatformWrapper;
import bio.terra.common.category.Unit;
import bio.terra.model.CloudPlatform;
import bio.terra.service.snapshotbuilder.query.TablePointer;
import bio.terra.service.snapshotbuilder.query.TableVariable;
import org.apache.commons.lang3.NotImplementedException;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@Tag(Unit.TAG)
class SearchConceptsQueryBuilderTest {
  @ParameterizedTest
  @EnumSource(CloudPlatform.class)
  void buildSearchConceptsQuery(CloudPlatform platform) {
    CloudPlatformWrapper platformWrapper = CloudPlatformWrapper.of(platform);
    String actual =
        SearchConceptsQueryBuilder.buildSearchConceptsQuery(
            "condition", "cancer", s -> s, CloudPlatformWrapper.of(platform));
    String expected =
        formatSQLWithLimit(
            "SELECT c.concept_name, c.concept_id, COUNT(DISTINCT c0.person_id) "
                + "FROM concept AS c  "
                + "JOIN condition_occurrence AS c0 "
                + "ON c0.condition_concept_id = c.concept_id "
                + "WHERE (c.domain_id = 'condition' "
                + "AND (CONTAINS_SUBSTR(c.concept_name, 'cancer') "
                + "OR CONTAINS_SUBSTR(c.concept_code, 'cancer')))",
            platformWrapper);
    assertThat(
        "generated SQL for search concepts query is correct",
        actual,
        equalToCompressingWhiteSpace(expected));
  }

  @ParameterizedTest
  @EnumSource(CloudPlatform.class)
  void buildSearchConceptsQueryEmpty(CloudPlatform platform) {
    CloudPlatformWrapper cloudPlatformWrapper = CloudPlatformWrapper.of(platform);
    String actual =
        SearchConceptsQueryBuilder.buildSearchConceptsQuery(
            "Condition", "", s -> s, CloudPlatformWrapper.of(platform));
    String expected =
        formatSQLWithLimit(
            "SELECT c.concept_name, c.concept_id FROM concept AS c WHERE c.domain_id = 'Condition'",
            cloudPlatformWrapper);
    assertThat(
        "generated SQL for empty search concepts query is correct",
        actual,
        equalToCompressingWhiteSpace(expected));
  }

  String formatSQLWithLimit(String sql, CloudPlatformWrapper cloudPlatformWrapper) {
    int limit = 100;
    if (cloudPlatformWrapper.isAzure()) {
      return String.format("TOP %d %s", limit, sql);
    } else if (cloudPlatformWrapper.isGcp()) {
      return String.format("%s LIMIT %d", sql, limit);
    } else {
      throw new NotImplementedException("Cloud platform not implemented");
    }
  }

  @ParameterizedTest
  @EnumSource(CloudPlatform.class)
  void testCreateSearchConceptClause(CloudPlatform platform) {
    TablePointer conceptTablePointer = TablePointer.fromTableName("concept", s -> s);
    TableVariable conceptTableVariable = TableVariable.forPrimary(conceptTablePointer);

    assertThat(
        "generated sql is as expected",
        SearchConceptsQueryBuilder.createSearchConceptClause(
                conceptTablePointer, conceptTableVariable, "cancer", "concept_name")
            .renderSQL(CloudPlatformWrapper.of(platform)),
        // table name is added when the Query is created
        equalTo("CONTAINS_SUBSTR(null.concept_name, 'cancer')"));
  }

  @ParameterizedTest
  @EnumSource(CloudPlatform.class)
  void testCreateDomainClause(CloudPlatform platform) {
    TablePointer conceptTablePointer = TablePointer.fromTableName("concept", s -> s);
    TableVariable conceptTableVariable = TableVariable.forPrimary(conceptTablePointer);

    assertThat(
        "generated sql is as expected",
        SearchConceptsQueryBuilder.createDomainClause(
                conceptTablePointer, conceptTableVariable, "cancer")
            .renderSQL(CloudPlatformWrapper.of(platform)),
        // table name is added when the Query is created
        equalTo("null.domain_id = 'cancer'"));
  }
}
