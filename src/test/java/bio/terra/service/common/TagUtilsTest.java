package bio.terra.service.common;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;

import bio.terra.common.TagUtils;
import bio.terra.common.category.Unit;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.springframework.test.context.ActiveProfiles;

@ActiveProfiles({"google", "unittest"})
@Category(Unit.class)
public class TagUtilsTest {

  @Test
  public void testDatasetRequestToDatasetTags() {
    assertThat("Null tag list is converted to empty list", TagUtils.getDistinctTags(null), empty());

    assertThat("Empty tag list is returned", TagUtils.getDistinctTags(List.of()), empty());

    List<String> nullTagElements = new ArrayList<>();
    nullTagElements.add(null);
    assertThat("Null tags are filtered out", TagUtils.getDistinctTags(nullTagElements), empty());

    String tag = "a tag";
    assertThat(
        "Duplicate tags are filtered out",
        TagUtils.getDistinctTags(List.of(tag, tag, tag)),
        contains(tag));

    List<String> multipleCasedTags = List.of(tag.toLowerCase(), tag.toUpperCase());
    assertThat(
        "Tags are case sensitive",
        TagUtils.getDistinctTags(multipleCasedTags),
        containsInAnyOrder(multipleCasedTags.toArray()));
  }
}
