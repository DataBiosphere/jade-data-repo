package bio.terra.common;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.springframework.stereotype.Component;

@Component
public class TagUtils {

  /**
   * @param requestedTags tags provided by a user at resource creation
   * @return distinct non-null tags from request (case-sensitive)
   */
  public static List<String> getDistinctTags(List<String> requestedTags) {
    return Optional.ofNullable(requestedTags).orElse(List.of()).stream()
        .distinct()
        .filter(Objects::nonNull)
        .toList();
  }
}
