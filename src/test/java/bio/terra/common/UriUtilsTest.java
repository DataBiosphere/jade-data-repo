package bio.terra.common;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import bio.terra.common.category.Unit;
import bio.terra.service.filedata.exception.InvalidUriException;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(Unit.class)
public class UriUtilsTest {

  @Test
  public void testGetQueryParameterValue() {
    assertThat(
        "query parameter is returned",
        UriUtils.getValueFromQueryParameter("http://foo/bar?q=123", "q"),
        equalTo("123"));
    assertThat(
        "query parameter is not found",
        UriUtils.getValueFromQueryParameter("http://foo/bar", "q"),
        is(nullValue()));
    assertThat(
        "multivalues are aggregated",
        UriUtils.getValueFromQueryParameter("http://foo/bar?q=123&q=456", "q"),
        equalTo("123,456"));
  }

  @Test
  public void testInvalidUris() {
    String badUri = "not a good uri";

    TestUtils.assertError(
        InvalidUriException.class,
        "Invalid URI",
        () -> UriUtils.getValueFromQueryParameter(badUri, "q"));

    TestUtils.assertError(
        InvalidUriException.class, "Invalid URI", () -> UriUtils.omitQueryParameter(badUri, "q"));
  }

  @Test
  public void testOmitQueryParameter() {
    assertThat(
        "query parameter is removed",
        UriUtils.omitQueryParameter("http://foo/bar?q=123", "q"),
        equalTo("http://foo/bar"));
    assertThat(
        "multiple query parameters are removed",
        UriUtils.omitQueryParameter("http://foo/bar?q=123&q=456", "q"),
        equalTo("http://foo/bar"));
    assertThat(
        "query parameter is untouched",
        UriUtils.omitQueryParameter("http://foo/bar?q=123", "notq"),
        equalTo("http://foo/bar?q=123"));
    assertThat(
        "only specified query parameter is removed",
        UriUtils.omitQueryParameter("http://foo/bar?q=123&a=4", "q"),
        equalTo("http://foo/bar?a=4"));
    // Fails when using UriComponentsBuilder
    assertThat(
        "try with a gs path",
        UriUtils.omitQueryParameter("gs://bucket_of_stuff/0/myblob.txt", "q"),
        equalTo("gs://bucket_of_stuff/0/myblob.txt"));
  }
}
