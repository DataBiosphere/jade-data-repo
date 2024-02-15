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
    assertThat(
        "generated SQL is correct",
        SearchConceptsQueryBuilder.buildSearchConceptsQuery(
            "condition", "cancer", s -> s, CloudPlatformWrapper.of(platform)),
        equalToCompressingWhiteSpace(
            "SELECT c.concept_name, c.concept_id FROM concept AS c "
                + "WHERE (c.domain_id = 'condition' "
                + "AND (CONTAINS_SUBSTR(c.concept_name, 'cancer') "
                + "OR CONTAINS_SUBSTR(c.concept_code, 'cancer'))) "
                + "LIMIT 100"));
  }

  @ParameterizedTest
  @EnumSource(CloudPlatform.class)
  void buildSearchConceptsQueryEmpty(CloudPlatform platform) {
    assertThat(
        "generated SQL for empty search string is correct",
        SearchConceptsQueryBuilder.buildSearchConceptsQuery(
            "Condition", "", s -> s, CloudPlatformWrapper.of(platform)),
        equalToCompressingWhiteSpace(
            "SELECT c.concept_name, c.concept_id FROM concept AS c "
                + "WHERE c.domain_id = 'Condition' "
                + "LIMIT 100"));
  }

  @ParameterizedTest
  @EnumSource(CloudPlatform.class)
  void testCreateSearchConceptClause(CloudPlatform platform) {
    TablePointer conceptTablePointer = TablePointer.fromTableName("concept", s -> s);
    TableVariable conceptTableVariable = TableVariable.forPrimary(conceptTablePointer);

    assertThat(
        "generated sql is as expected",
        createSearchConceptClause(
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
        createDomainClause(conceptTablePointer, conceptTableVariable, "cancer")
            .renderSQL(CloudPlatformWrapper.of(platform)),
        // table name is added when the Query is created
        equalTo("null.domain_id = 'cancer'"));
  }
}
