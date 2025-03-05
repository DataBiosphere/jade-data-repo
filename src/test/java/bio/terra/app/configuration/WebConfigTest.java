package bio.terra.app.configuration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import bio.terra.common.category.Unit;
import java.io.IOException;
import java.util.Arrays;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;

@Tag(Unit.TAG)
class WebConfigTest {

  @Test
  void swaggerUiVersionMatchesClasspath() throws IOException {
    PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
    Resource[] resources =
        resolver.getResources("classpath:/META-INF/resources/webjars/swagger-ui-dist/*/");

    Pattern versionPattern = Pattern.compile(".+/swagger-ui-dist/(\\d+\\.\\d+\\.\\d+)/");
    Optional<String> currentVersion =
        Arrays.stream(resources)
            .map(
                resource -> {
                  Matcher matcher =
                      versionPattern.matcher(((ClassPathResource) resource).getPath());
                  return matcher.matches() ? matcher.group(1) : null;
                })
            .filter(v -> v != null)
            .findFirst();

    assertTrue(
        currentVersion.isPresent(),
        "Could not find swagger-ui-dist version in classpath. Check your build.gradle dependencies.");

    assertEquals(
        currentVersion.get(),
        WebConfig.SWAGGER_UI_VERSION,
        "SWAGGER_UI_VERSION in WebConfig.java does not match the version in build.gradle. "
            + "Update the version in WebConfig.java to "
            + currentVersion.get()
            + " to match.");
  }
}
