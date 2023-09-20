package bio.terra.service.common.gcs;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import bio.terra.common.category.Unit;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.springframework.test.context.ActiveProfiles;

@ActiveProfiles({"google", "unittest"})
@Category(Unit.class)
public class GcsUriUtilsTest {

  @Test
  public void testGsPathExtraction() {
    assertThat(
        GcsUriUtils.parseBlobUri("gs://mybucket/my/path.txt"),
        equalTo(BlobId.of("mybucket", "my/path.txt")));
  }

  @Test
  public void testGsPathExtractionSpecialCharacters() {
    assertThat(
        GcsUriUtils.parseBlobUri("gs://mybucket/my/path# space %.txt"),
        equalTo(BlobId.of("mybucket", "my/path# space %.txt")));
  }

  @Test
  public void testGsPathExtractionBucketTooShort() {
    Exception exception =
        assertThrows(IllegalArgumentException.class, () -> GcsUriUtils.parseBlobUri("gs://a/b"));
    assertEquals("Invalid bucket name in gs path: 'gs://a/b'", exception.getMessage());
  }

  @Test
  public void testGsPathExtractionBucketTooLong() {
    final String bucketName = "x".repeat(250);
    final String pathName = String.format("gs://%s/path", bucketName);
    Exception exception =
        assertThrows(IllegalArgumentException.class, () -> GcsUriUtils.parseBlobUri(pathName));
    assertEquals(
        String.format("Invalid bucket name in gs path: '%s'", pathName), exception.getMessage());
  }

  @Test
  public void testGsPathExtractionBucketComponentTooLong() {
    final String bucketComponent = "x".repeat(64);
    final String pathName = String.format("gs://abc.%s/path", bucketComponent);
    Exception exception =
        assertThrows(IllegalArgumentException.class, () -> GcsUriUtils.parseBlobUri(pathName));
    assertEquals(
        String.format("Component name '%s' too long in gs path: '%s'", bucketComponent, pathName),
        exception.getMessage());
  }

  @Test
  public void testParseValidPatternAtEnd() {
    BlobId parsed = GcsUriUtils.parseBlobUri("gs://some-bucket/some/prefix*");
    assertThat("bucket is extracted", parsed.getBucket(), equalTo("some-bucket"));
    assertThat("path is extracted", parsed.getName(), equalTo("some/prefix*"));
  }

  @Test
  public void testParseValidPatternInMiddle() {
    BlobId parsed = GcsUriUtils.parseBlobUri("gs://some-bucket/some*pattern");
    assertThat("bucket is extracted", parsed.getBucket(), equalTo("some-bucket"));
    assertThat("path is extracted", parsed.getName(), equalTo("some*pattern"));
  }

  @Test
  public void testInvalidBucketWildcard() {
    String uri = "gs://some-bucket-*/some/file/path";
    Exception exception =
        assertThrows(IllegalArgumentException.class, () -> GcsUriUtils.parseBlobUri(uri));
    assertEquals("Bucket wildcards are not supported: URI: '" + uri + "'", exception.getMessage());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidMultiWildcard() {
    GcsUriUtils.parseBlobUri("gs://some-bucket/some/prefix*/some*pattern");
  }

  @Test
  public void testCreatesGsPathFromBlob() {
    final String bucket = "my-bucket";
    final String name = "foo/bar/baz.json";
    final String gsPath = String.format("gs://%s/%s", bucket, name);
    BlobInfo blobInfo = Blob.newBuilder(bucket, name).build();
    assertEquals(gsPath, GcsUriUtils.getGsPathFromBlob(blobInfo));
  }

  @Test
  public void testIsValidGcsUri() {
    assertTrue("is a gs uri", GcsUriUtils.isGsUri("gs://bucket/blob"));
    assertFalse("is not a uri", GcsUriUtils.isGsUri("https://bucket/blob"));
  }
}
