package bio.terra.service.snapshotbuilder.utils;

import static org.hamcrest.MatcherAssert.assertThat;

import bio.terra.common.CloudPlatformWrapper;
import bio.terra.common.category.Unit;
import bio.terra.model.CloudPlatform;
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
            101, s -> s, CloudPlatformWrapper.of(platform));
    String expected =
        """
        SELECT c.concept_name, c.concept_id
        FROM concept AS c
        WHERE (c.concept_id IN
          (SELECT c.concept_id_2 FROM concept_relationship AS c
          WHERE (c.concept_id_1 = 101 AND c.relationship_id = 'Subsumes')) AND c.standard_concept = 'S')""";
    assertThat(sql, Matchers.equalToCompressingWhiteSpace(expected));
  }
}
