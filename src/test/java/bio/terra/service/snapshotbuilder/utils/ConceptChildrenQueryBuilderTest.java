package bio.terra.service.snapshotbuilder.utils;

import static org.hamcrest.MatcherAssert.assertThat;

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
  @ParameterizedTest
  @EnumSource(CloudPlatform.class)
  void buildConceptChildrenQuery(CloudPlatform platform) {
    String sql =
        ConceptChildrenQueryBuilder.buildConceptChildrenQuery(
            new SnapshotBuilderDomainOption(), 101, s -> s, CloudPlatformWrapper.of(platform));
    String expected =
        """
        SELECT c.concept_name, c.concept_id
        FROM concept AS c
        WHERE c.concept_id IN
          (SELECT c.descendant_concept_id FROM concept_ancestor AS c
          WHERE c.ancestor_concept_id = 101)""";
    assertThat(sql, Matchers.equalToCompressingWhiteSpace(expected));
  }
}
