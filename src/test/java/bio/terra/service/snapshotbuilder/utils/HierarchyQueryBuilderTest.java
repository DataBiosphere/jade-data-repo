package bio.terra.service.snapshotbuilder.utils;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalToCompressingWhiteSpace;

import bio.terra.common.category.Unit;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(Unit.TAG)
class HierarchyQueryBuilderTest {

  @Test
  void generateQuery() {
    var query = new HierarchyQueryBuilder(x -> x).generateQuery(1);
    var expected =
        """
            SELECT
              c.concept_id_1 AS parent_id, c.concept_id_2 AS concept_id, c0.concept_name, c0.concept_code
            FROM concept_relationship AS c
              JOIN concept AS c0 ON c0.concept_id = c.concept_id_2
              JOIN concept AS c1 ON c1.concept_id = c.concept_id_1
            WHERE
              (c.concept_id_1 IN (SELECT c.ancestor_concept_id FROM concept_ancestor AS c
                                  WHERE (c.descendant_concept_id = 1 AND c.ancestor_concept_id != 1))
              AND c.relationship_id = 'Subsumes' AND c1.standard_concept IS NOT NULL AND c0.standard_concept IS NOT NULL)""";
    assertThat(query.renderSQL(null), equalToCompressingWhiteSpace(expected));
  }
}
