package bio.terra.service.snapshotbuilder.utils;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalToCompressingWhiteSpace;

import bio.terra.common.CloudPlatformWrapper;
import bio.terra.common.category.Unit;
import bio.terra.model.CloudPlatform;
import bio.terra.model.SnapshotBuilderDomainOption;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

@Tag(Unit.TAG)
class ConceptChildrenQueryBuilderTest {

  private SnapshotBuilderDomainOption createDomainOption(
      String name, int id, String occurrenceTable, String columnName) {
    var option = new SnapshotBuilderDomainOption();
    option.name(name).id(id).tableName(occurrenceTable).columnName(columnName);
    return option;
  }

  @ParameterizedTest
  @EnumSource(CloudPlatform.class)
  void buildConceptChildrenQuery(CloudPlatform platform) {
    SnapshotBuilderDomainOption domainOption =
        createDomainOption("Condition", 19, "condition_occurrence", "condition_concept_id");
    String sql =
        new QueryBuilderFactory()
            .conceptChildrenQueryBuilder(s -> s)
            .buildConceptChildrenQuery(domainOption, 101)
            .renderSQL(CloudPlatformWrapper.of(platform));
    String expected =
        """
       SELECT c.concept_name, c.concept_id, COUNT(DISTINCT c1.person_id) AS count
       FROM concept AS c
       JOIN concept_ancestor AS c0
       ON c0.ancestor_concept_id = c.concept_id
       JOIN condition_occurrence AS c1
       ON c1.condition_concept_id = c0.descendant_concept_id
       WHERE (c.concept_id IN
       (SELECT c.concept_id_2 FROM concept_relationship AS c WHERE (c.concept_id_1 = 101 AND c.relationship_id = 'Subsumes')) AND c.standard_concept = 'S')
       GROUP BY c.concept_name, c.concept_id
       ORDER BY c.concept_name ASC
        """;
    assertThat(sql, equalToCompressingWhiteSpace(expected));
  }

  @ParameterizedTest
  @EnumSource(CloudPlatform.class)
  void buildGetDomainIdQuery(CloudPlatform platform) {
    String sql =
        new QueryBuilderFactory()
            .conceptChildrenQueryBuilder(s -> s)
            .retrieveDomainId(101)
            .renderSQL(CloudPlatformWrapper.of(platform));
    String expected =
        """
        SELECT c.domain_id
        FROM concept AS c
        WHERE c.concept_id = 101""";
    assertThat(sql, Matchers.equalToCompressingWhiteSpace(expected));
  }
}
