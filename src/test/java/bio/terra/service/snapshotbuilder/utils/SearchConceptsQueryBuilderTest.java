package bio.terra.service.snapshotbuilder.utils;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalToCompressingWhiteSpace;

import bio.terra.common.category.Unit;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@Tag(Unit.TAG)
class SearchConceptsQueryBuilderTest {

  @Test
  void buildSearchConceptsQuery() {
    assertThat(
        "generated SQL is correct",
        SearchConceptsQueryBuilder.buildSearchConceptsQuery("condition", "cancer", s -> s),
        equalToCompressingWhiteSpace(
            "SELECT c.concept_name, c.concept_id FROM concept AS c "
                + "WHERE (c.domain_id = 'condition' "
                + "AND (CONTAINS_SUBSTR(c.concept_name, 'cancer') "
                + "OR CONTAINS_SUBSTR(c.concept_code, 'cancer')))"));
  }
}
