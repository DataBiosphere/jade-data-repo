package bio.terra.service.snapshotbuilder.utils;

import static bio.terra.service.snapshotbuilder.utils.SearchConceptsQueryBuilder.createDomainClause;
import static bio.terra.service.snapshotbuilder.utils.SearchConceptsQueryBuilder.createSearchConceptClause;
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
    if (platformWrapper.isGcp()) {
      assertThat(
          "generated SQL for GCP is correct",
          actual,
          equalToCompressingWhiteSpace(
              "SELECT c.concept_name, c.concept_id FROM concept AS c "
                  + "WHERE (c.domain_id = 'condition' "
                  + "AND (CONTAINS_SUBSTR(c.concept_name, 'cancer') "
                  + "OR CONTAINS_SUBSTR(c.concept_code, 'cancer'))) "
                  + "LIMIT 100"));
    }
    if (platformWrapper.isAzure()) {
      assertThat(
          "generated SQL for Azure is correct",
          actual,
          equalToCompressingWhiteSpace(
              "TOP 100 SELECT c.concept_name, c.concept_id FROM concept AS c "
                  + "WHERE (c.domain_id = 'condition' "
                  + "AND (CHARINDEX('cancer', c.concept_name) > 0 "
                  + "OR CHARINDEX('cancer', c.concept_code) > 0))"));
    }
  }

  @ParameterizedTest
  @EnumSource(CloudPlatform.class)
  void buildSearchConceptsQueryEmpty(CloudPlatform platform) {
    CloudPlatformWrapper platformWrapper = CloudPlatformWrapper.of(platform);
    String actual =
        SearchConceptsQueryBuilder.buildSearchConceptsQuery(
            "Condition", "", s -> s, CloudPlatformWrapper.of(platform));
    String expected =
        "SELECT c.concept_name, c.concept_id FROM concept AS c "
            + "WHERE c.domain_id = 'Condition' ";
    if (platformWrapper.isAzure()) {
      assertThat(
          "generated SQL for Azure empty search string is correct",
          actual,
          equalToCompressingWhiteSpace("TOP 100 " + expected));
    }
    if (platformWrapper.isGcp()) {
      assertThat(
          "generated SQL for GCP empty search string is correct",
          actual,
          equalToCompressingWhiteSpace(expected + "LIMIT 100"));
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
          equalTo("CHARINDEX('cancer', null.concept_name) > 0"));
    }
    if (platformWrapper.isGcp()) {
      assertThat(
          "generated sql is as expected",
          actual,
          // table name is added when the Query is created
          equalTo("CONTAINS_SUBSTR(null.concept_name, 'cancer')"));
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
        equalTo("null.domain_id = 'cancer'"));
  }
}
