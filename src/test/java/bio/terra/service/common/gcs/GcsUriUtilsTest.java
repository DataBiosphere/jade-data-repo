package bio.terra.service.common.gcs;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

import bio.terra.common.category.Unit;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(Unit.TAG)
class GcsUriUtilsTest {

  @Test
  void testGsPathExtraction() {
    assertThat(
        GcsUriUtils.parseBlobUri("gs://mybucket/my/path.txt"),
        equalTo(BlobId.of("mybucket", "my/path.txt")));
  }

  @Test
  void testGsPathExtractionSpecialCharacters() {
    assertThat(
        GcsUriUtils.parseBlobUri("gs://mybucket/my/path# space %.txt"),
        equalTo(BlobId.of("mybucket", "my/path# space %.txt")));
  }

  @Test
  void testGsPathExtractionBucketTooShort() {
    Exception exception =
        assertThrows(IllegalArgumentException.class, () -> GcsUriUtils.parseBlobUri("gs://a/b"));
    assertThat(exception.getMessage(), is("Invalid bucket name in gs path: 'gs://a/b'"));
  }

  @Test
  void testGsPathExtractionBucketTooLong() {
    final String bucketName = "x".repeat(250);
    final String pathName = String.format("gs://%s/path", bucketName);
    Exception exception =
        assertThrows(IllegalArgumentException.class, () -> GcsUriUtils.parseBlobUri(pathName));
    assertThat(
        exception.getMessage(),
        is(String.format("Invalid bucket name in gs path: '%s'", pathName)));
  }

  @Test
  void testGsPathExtractionBucketComponentTooLong() {
    final String bucketComponent = "x".repeat(64);
    final String pathName = String.format("gs://abc.%s/path", bucketComponent);
    Exception exception =
        assertThrows(IllegalArgumentException.class, () -> GcsUriUtils.parseBlobUri(pathName));
    assertThat(
        exception.getMessage(),
        is(
            String.format(
                "Component name '%s' too long in gs path: '%s'", bucketComponent, pathName)));
  }

  @Test
  void testParseValidPatternAtEnd() {
    BlobId parsed = GcsUriUtils.parseBlobUri("gs://some-bucket/some/prefix*");
    assertThat("bucket is extracted", parsed.getBucket(), equalTo("some-bucket"));
    assertThat("path is extracted", parsed.getName(), equalTo("some/prefix*"));
  }

  @Test
  void testParseValidPatternInMiddle() {
    BlobId parsed = GcsUriUtils.parseBlobUri("gs://some-bucket/some*pattern");
    assertThat("bucket is extracted", parsed.getBucket(), equalTo("some-bucket"));
    assertThat("path is extracted", parsed.getName(), equalTo("some*pattern"));
  }

  @Test
  void testInvalidBucketWildcard() {
    String uri = "gs://some-bucket-*/some/file/path";
    Exception exception =
        assertThrows(IllegalArgumentException.class, () -> GcsUriUtils.parseBlobUri(uri));
    assertThat(
        exception.getMessage(), is("Bucket wildcards are not supported: URI: '" + uri + "'"));
  }

  @Test
  void testInvalidMultiWildcard() {
    assertThrows(
        IllegalArgumentException.class,
        () -> GcsUriUtils.parseBlobUri("gs://some-bucket/some/prefix*/some*pattern"));
  }

  @Test
  void testCreatesGsPathFromBlob() {
    final String bucket = "my-bucket";
    final String name = "foo/bar/baz.json";
    final String gsPath = String.format("gs://%s/%s", bucket, name);
    BlobInfo blobInfo = BlobInfo.newBuilder(bucket, name).build();
    assertThat(GcsUriUtils.getGsPathFromBlob(blobInfo), is(gsPath));
  }

  @Test
  void testIsValidGcsUri() {
    assertThat("is a gs uri", GcsUriUtils.isGsUri("gs://bucket/blob"));
    assertThat("is not a uri", !GcsUriUtils.isGsUri("https://bucket/blob"));
  }
}
