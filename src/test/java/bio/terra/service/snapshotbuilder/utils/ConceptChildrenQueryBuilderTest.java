package bio.terra.service.snapshotbuilder.utils;

import static org.hamcrest.MatcherAssert.assertThat;

import bio.terra.common.category.Unit;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(Unit.TAG)
class ConceptChildrenQueryBuilderTest {
  @Test
  void buildConceptChildrenQuery() {
    String sql = ConceptChildrenQueryBuilder.buildConceptChildrenQuery(100, s -> s);
    String expected =
        """
        SELECT c.concept_name, c.concept_id FROM concept AS c
        WHERE c.concept_id IN
          (SELECT c.descendant_concept_id FROM concept_ancestor AS c
          WHERE c.ancestor_concept_id = 100)""";
    assertThat(sql, Matchers.equalToCompressingWhiteSpace(expected));
  }
}
