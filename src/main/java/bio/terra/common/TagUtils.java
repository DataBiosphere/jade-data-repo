package bio.terra.common;

import java.util.List;
import java.util.Objects;
import org.apache.commons.collections4.ListUtils;
import org.springframework.stereotype.Component;

@Component
public class TagUtils {

  /**
   * @param requestedTags tags provided by a user at resource creation
   * @return distinct non-null tags from request (case-sensitive)
   */
  public static List<String> getDistinctTags(List<String> requestedTags) {
    return ListUtils.emptyIfNull(requestedTags).stream()
        .distinct()
        .filter(Objects::nonNull)
        .toList();
  }
}
