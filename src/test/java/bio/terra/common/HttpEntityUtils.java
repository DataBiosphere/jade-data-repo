package bio.terra.common;

import java.util.List;
import java.util.Optional;
import org.apache.http.HttpHeaders;
import org.springframework.http.HttpEntity;

/**
 * Test utilities for examining HttpEntities within tests, to verify that authentication calls are
 * being made as expected.
 */
public class HttpEntityUtils {

  /**
   * @return true if the entity contains no authorizations
   */
  public static <T> boolean isUnauthenticated(HttpEntity<T> httpEntity) {
    return getAuthorizations(httpEntity).isEmpty();
  }

  /**
   * @return true if the entity contains the given bearer token as its sole means of authorization
   */
  public static <T> boolean hasToken(HttpEntity<T> httpEntity, String token) {
    String bearerHeader = "Bearer %s".formatted(token);
    List<String> authorizations = getAuthorizations(httpEntity);
    return authorizations.size() == 1 && authorizations.get(0).equals(bearerHeader);
  }

  private static <T> List<String> getAuthorizations(HttpEntity<T> httpEntity) {
    return Optional.ofNullable(httpEntity.getHeaders().get(HttpHeaders.AUTHORIZATION))
        .orElse(List.of());
  }
}
