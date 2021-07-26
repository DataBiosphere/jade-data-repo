package bio.terra.service.common.gcs;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hamcrest.Matchers.equalTo;

import bio.terra.common.category.Unit;
import bio.terra.service.dataset.exception.InvalidUriException;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobInfo;
import org.apache.commons.lang3.StringUtils;
import org.assertj.core.api.Assertions;
import org.hamcrest.MatcherAssert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(Unit.class)
public class GcsUriUtilsTest {

  @Test
  public void testGsPathExtraction() {
    Assertions.assertThat(GcsUriUtils.parseBlobUri("gs://mybucket/my/path.txt"))
        .usingRecursiveComparison()
        .isEqualTo(new GcsUriUtils.GsUrlParts().bucket("mybucket").path("my/path.txt"));
  }

  @Test
  public void testGsPathExtractionSpecialCharacters() {
    assertThat(GcsUriUtils.parseBlobUri("gs://mybucket/my/path# space %.txt"))
        .usingRecursiveComparison()
        .isEqualTo(new GcsUriUtils.GsUrlParts().bucket("mybucket").path("my/path# space %.txt"));
  }

  @Test
  public void testGsPathExtractionNoPath() {
    assertThatThrownBy(() -> GcsUriUtils.parseBlobUri("gs://mybucket"))
        .hasMessage("Missing object name in gs path: 'gs://mybucket'");
  }

  @Test
  public void testGsPathExtractionInvalidBucketCharacters() {
    assertThatThrownBy(() -> GcsUriUtils.parseBlobUri("gs://my#bucket"))
        .hasMessage("Invalid bucket name in gs path: 'gs://my#bucket'");
  }

  @Test
  public void testGsPathExtractionInvalidBucketCharactersCaps() {
    assertThatThrownBy(() -> GcsUriUtils.parseBlobUri("gs://MYBUCKET"))
        .hasMessage("Invalid bucket name in gs path: 'gs://MYBUCKET'");
  }

  @Test
  public void testGsPathExtractionBucketTooShort() {
    assertThatThrownBy(() -> GcsUriUtils.parseBlobUri("gs://a/b"))
        .hasMessage("Invalid bucket name in gs path: 'gs://a/b'");
  }

  @Test
  public void testGsPathExtractionBucketTooLong() {
    final String bucketName = StringUtils.rightPad("abc", 250, 'x');
    final String pathName = String.format("gs://%s/path", bucketName);
    assertThatThrownBy(() -> GcsUriUtils.parseBlobUri(pathName))
        .hasMessage(String.format("Invalid bucket name in gs path: '%s'", pathName));
  }

  @Test
  public void testGsPathExtractionBucketComponentTooLong() {
    final String bucketComponent = StringUtils.rightPad("a", 64, 'x');
    final String pathName = String.format("gs://abc.%s/path", bucketComponent);
    assertThatThrownBy(() -> GcsUriUtils.parseBlobUri(pathName))
        .hasMessage(
            String.format(
                "Component name '%s' too long in gs path: '%s'", bucketComponent, pathName));
  }

  @Test
  public void testParseValidPatternAtEnd() {
    GcsUriUtils.GsUrlParts parsed = GcsUriUtils.parseBlobUri("gs://some-bucket/some/prefix*");
    MatcherAssert.assertThat("bucket is extracted", parsed.getBucket(), equalTo("some-bucket"));
    MatcherAssert.assertThat("path is extracted", parsed.getPath(), equalTo("some/prefix*"));
    MatcherAssert.assertThat("not a wildcard", parsed.getIsWildcard(), equalTo(true));
  }

  @Test
  public void testParseValidPatternInMiddle() {
    GcsUriUtils.GsUrlParts parsed = GcsUriUtils.parseBlobUri("gs://some-bucket/some*pattern");
    MatcherAssert.assertThat("bucket is extracted", parsed.getBucket(), equalTo("some-bucket"));
    MatcherAssert.assertThat("path is extracted", parsed.getPath(), equalTo("some*pattern"));
    MatcherAssert.assertThat("not a wildcard", parsed.getIsWildcard(), equalTo(true));
  }

  @Test(expected = InvalidUriException.class)
  public void testNotAGsUri() {
    GcsUriUtils.parseBlobUri("https://foo.com/bar");
  }

  @Test(expected = InvalidUriException.class)
  public void testInvalidBucketWildcard() {
    GcsUriUtils.parseBlobUri("gs://some-bucket-*/some/file/path");
  }

  @Test(expected = InvalidUriException.class)
  public void testInvalidMultiWildcard() {
    GcsUriUtils.parseBlobUri("gs://some-bucket/some/prefix*/some*pattern");
  }

  @Test
  public void testCreatesGsPathFromBlob() {
    final String bucket = "my-bucket";
    final String name = "foo/bar/baz.json";
    final String gsPath = String.format("gs://%s/%s", bucket, name);
    BlobInfo blobInfo = Blob.newBuilder(bucket, name).build();
    assertThat(gsPath).isEqualTo(GcsUriUtils.getGsPathFromBlob(blobInfo));
  }
}
