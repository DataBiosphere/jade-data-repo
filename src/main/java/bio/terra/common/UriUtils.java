package bio.terra.common;

import autovalue.shaded.kotlin.text.Charsets;
import bio.terra.service.filedata.exception.InvalidUriException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Objects;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.client.utils.URLEncodedUtils;

/** Handy methods for working with URIs */
public class UriUtils {

  private UriUtils() {}

  /**
   * Extract the value from a query parameter
   *
   * @param uri a valid uri
   * @param parameter the name of the parameter whose value to read
   * @return the value of the query parameter or null. If there are multiple instances of the query
   *     parameter, return the value as a comma-separated list
   */
  public static String getValueFromQueryParameter(String uri, String parameter) {
    List<String> values =
        URLEncodedUtils.parse(toUri(uri), Charsets.UTF_8).stream()
            .filter(p -> Objects.equals(p.getName(), parameter))
            .map(NameValuePair::getValue)
            .toList();
    // Explicitly return null if there were no matching query parameters
    if (values.isEmpty()) {
      return null;
    }
    return String.join(",", values);
  }

  /**
   * Rewrite the uri with the specified query parameter removed. Note: the order of any existing
   * parameters may not be maintained
   *
   * @param uri the uri to rewrite
   * @param parameter the parameter to exclude
   * @return a new uri string any reference to the query parameter removed
   */
  public static String omitQueryParameter(String uri, String parameter) {
    URI initUri = toUri(uri);
    List<NameValuePair> newQueryParams =
        URLEncodedUtils.parse(toUri(uri), Charsets.UTF_8).stream()
            .filter(p -> !Objects.equals(p.getName(), parameter))
            .toList();
    return new URIBuilder(initUri).setParameters(newQueryParams).toString();
  }

  /**
   * Convert a string to a {@link URI} and throw an unchecked exception if the creation fails
   *
   * @param uri The URI to concert
   * @return A new {@link URI}
   * @throws InvalidUriException if the creation of the {@link URI} fails
   */
  public static URI toUri(String uri) throws InvalidUriException {
    try {
      return new URI(uri);
    } catch (URISyntaxException e) {
      throw new InvalidUriException("Invalid URI", e);
    }
  }
}
