package bio.terra.app.configuration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import bio.terra.common.category.Unit;
import java.io.IOException;
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

    assertEquals(
        1, resources.length, "Expected one swagger-ui-dist resource, found " + resources.length);

    Pattern versionPattern = Pattern.compile(".+/swagger-ui-dist/(\\d+\\.\\d+\\.\\d+)/");
    Matcher matcher = versionPattern.matcher(((ClassPathResource) resources[0]).getPath());
    Optional<String> currentVersion = matcher.results().findFirst().map(m -> m.group(1));

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
