package bio.terra.service.tags;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import bio.terra.common.category.Unit;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;

@Tag(Unit.TAG)
class TagUtilsTest {

  @Test
  void testSanitizeTags() {
    assertThat("Null tag list is converted to empty list", TagUtils.sanitizeTags(null), empty());

    assertThat("Empty tag list is returned", TagUtils.sanitizeTags(List.of()), empty());

    assertThat(
        "Null, empty, and whitespace tags are filtered out",
        TagUtils.sanitizeTags(Arrays.asList("", "\t", "\n", null)),
        empty());

    String tag = "a tag";
    assertThat(
        "Duplicate tags are filtered out",
        TagUtils.sanitizeTags(List.of(tag, tag, tag)),
        contains(tag));

    assertThat(
        "Tags are deduplicated after whitespace is stripped",
        TagUtils.sanitizeTags(List.of(tag, tag + " ", " " + tag)),
        contains(tag));

    List<String> multipleCasedTags = List.of(tag.toLowerCase(), tag.toUpperCase());
    assertThat(
        "Tags are case sensitive",
        TagUtils.sanitizeTags(multipleCasedTags),
        containsInAnyOrder(multipleCasedTags.toArray()));
  }

  @Test
  void testAddTagsClause() throws SQLException {
    Connection connection = mock(Connection.class);
    List<String> clauses = new ArrayList<>();
    MapSqlParameterSource params = new MapSqlParameterSource();
    String table = "aTableName";

    TagUtils.addTagsClause(connection, List.of(), params, clauses, table);

    assertThat(clauses, contains("COALESCE(%s.tags, ARRAY[]::TEXT[]) @> :tags".formatted(table)));
    assertTrue(params.hasValue("tags"));
  }
}
