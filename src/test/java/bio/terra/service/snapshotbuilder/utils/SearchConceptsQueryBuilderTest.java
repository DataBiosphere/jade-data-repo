package bio.terra.service.snapshotbuilder.utils;

import static bio.terra.service.snapshotbuilder.utils.SearchConceptsQueryBuilder.createSearchConceptClause;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalToCompressingWhiteSpace;

import bio.terra.common.CloudPlatformWrapper;
import bio.terra.common.category.Unit;
import bio.terra.model.CloudPlatform;
import bio.terra.model.SnapshotBuilderDomainOption;
import bio.terra.service.snapshotbuilder.query.TablePointer;
import bio.terra.service.snapshotbuilder.query.TableVariable;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
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
  @EnumSource(CloudPlatform.class)
  void buildSearchConceptsQuery(CloudPlatform platform) {
    CloudPlatformWrapper platformWrapper = CloudPlatformWrapper.of(platform);
    SnapshotBuilderDomainOption domainOption =
        createDomainOption("Observation", 27, "observation", "observation_concept_id");
    String actual =
        SearchConceptsQueryBuilder.buildSearchConceptsQuery(
            domainOption, "cancer", s -> s, CloudPlatformWrapper.of(platform));

    if (platformWrapper.isGcp()) {
      assertThat(
          "generated SQL for GCP is correct",
          actual,
          equalToCompressingWhiteSpace(
              "SELECT c.concept_name, c.concept_id, COUNT(DISTINCT o.person_id) AS count "
                  + "FROM concept AS c "
                  + "JOIN concept_ancestor AS c0 ON c0.ancestor_concept_id = c.concept_id "
                  + "LEFT JOIN observation AS o ON o.observation_concept_id = c0.descendant_concept_id "
                  + "WHERE (c.domain_id = 'Observation' "
                  + "AND (CONTAINS_SUBSTR(c.concept_name, 'cancer') "
                  + "OR CONTAINS_SUBSTR(c.concept_code, 'cancer'))) "
                  + "GROUP BY c.concept_name, c.concept_id "
                  + "ORDER BY count DESC "
                  + "LIMIT 100"));
    }
    if (platformWrapper.isAzure()) {
      assertThat(
          "generated SQL for Azure is correct",
          actual,
          equalToCompressingWhiteSpace(
              "SELECT TOP 100 c.concept_name, c.concept_id, COUNT(DISTINCT o.person_id) AS count "
                  + "FROM concept AS c  "
                  + "JOIN concept_ancestor AS c0 ON c0.ancestor_concept_id = c.concept_id "
                  + "LEFT JOIN observation AS o ON o.observation_concept_id = c0.descendant_concept_id "
                  + "WHERE (c.domain_id = 'Observation' "
                  + "AND (CHARINDEX('cancer', c.concept_name) > 0 "
                  + "OR CHARINDEX('cancer', c.concept_code) > 0)) "
                  + "GROUP BY c.concept_name, c.concept_id "
                  + "ORDER BY count DESC"));
    }
  }

  @ParameterizedTest
  @EnumSource(CloudPlatform.class)
  void buildSearchConceptsQueryEmpty(CloudPlatform platform) {
    CloudPlatformWrapper platformWrapper = CloudPlatformWrapper.of(platform);
    SnapshotBuilderDomainOption domainOption =
        createDomainOption("Condition", 19, "condition_occurrence", "condition_concept_id");
    String actual =
        SearchConceptsQueryBuilder.buildSearchConceptsQuery(
            domainOption, "", s -> s, CloudPlatformWrapper.of(platform));
    String expected =
        "c.concept_name, c.concept_id, COUNT(DISTINCT c1.person_id) AS count "
            + "FROM concept AS c  "
            + "JOIN concept_ancestor AS c0 ON c0.ancestor_concept_id = c.concept_id "
            + "LEFT JOIN condition_occurrence AS c1 ON c1.condition_concept_id = c0.descendant_concept_id "
            + "WHERE c.domain_id = 'Condition' "
            + "GROUP BY c.concept_name, c.concept_id "
            + "ORDER BY count DESC";
    if (platformWrapper.isAzure()) {
      assertThat(
          "generated SQL for Azure empty search string is correct",
          actual,
          equalToCompressingWhiteSpace("SELECT TOP 100 " + expected));
    }
    if (platformWrapper.isGcp()) {
      assertThat(
          "generated SQL for GCP empty search string is correct",
          actual,
          equalToCompressingWhiteSpace("SELECT " + expected + " LIMIT 100"));
    }
  }

  @ParameterizedTest
  @EnumSource(CloudPlatform.class)
  void testCreateSearchConceptClause(CloudPlatform platform) {
    CloudPlatformWrapper platformWrapper = CloudPlatformWrapper.of(platform);
    TablePointer conceptTablePointer = TablePointer.fromTableName("concept", s -> s);
    TableVariable conceptTableVariable = TableVariable.forPrimary(conceptTablePointer);
    String actual =
        createSearchConceptClause(
                conceptTablePointer, conceptTableVariable, "cancer", "concept_name")
            .renderSQL(CloudPlatformWrapper.of(platform));
    if (platformWrapper.isAzure()) {
      assertThat(
          "generated sql is as expected",
          actual,
          // table name is added when the Query is created
          equalToCompressingWhiteSpace("CHARINDEX('cancer', null.concept_name) > 0"));
    }
    if (platformWrapper.isGcp()) {
      assertThat(
          "generated sql is as expected",
          actual,
          // table name is added when the Query is created
          equalToCompressingWhiteSpace("CONTAINS_SUBSTR(null.concept_name, 'cancer')"));
    }
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
        equalToCompressingWhiteSpace("null.domain_id = 'cancer'"));
  }
}
