package bio.terra.datarepo.service.filedata.google.gcs;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import bio.terra.datarepo.common.category.Unit;
import org.apache.commons.lang3.StringUtils;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(Unit.class)
public class GcsPdaoUnitTest {

  @Test
  public void testGsPathExtraction() {
    assertThat(GcsPdao.getGcsLocatorFromGsPath("gs://mybucket/my/path.txt"))
        .isEqualTo(new GcsPdao.GcsLocator("mybucket", "my/path.txt"));
  }

  @Test
  public void testGsPathExtractionSpecialCharacters() {
    assertThat(GcsPdao.getGcsLocatorFromGsPath("gs://mybucket/my/path# space %.txt"))
        .isEqualTo(new GcsPdao.GcsLocator("mybucket", "my/path# space %.txt"));
  }

  @Test
  public void testGsPathExtractionNoPath() {
    assertThatThrownBy(() -> GcsPdao.getGcsLocatorFromGsPath("gs://mybucket"))
        .hasMessage("Missing object name in gs path: 'gs://mybucket'");
  }

  @Test
  public void testGsPathExtractionInvalidBucketCharacters() {
    assertThatThrownBy(() -> GcsPdao.getGcsLocatorFromGsPath("gs://my#bucket"))
        .hasMessage("Invalid bucket name in gs path: 'gs://my#bucket'");
  }

  @Test
  public void testGsPathExtractionInvalidBucketCharactersCaps() {
    assertThatThrownBy(() -> GcsPdao.getGcsLocatorFromGsPath("gs://MYBUCKET"))
        .hasMessage("Invalid bucket name in gs path: 'gs://MYBUCKET'");
  }

  @Test
  public void testGsPathExtractionBucketTooShort() {
    assertThatThrownBy(() -> GcsPdao.getGcsLocatorFromGsPath("gs://a/b"))
        .hasMessage("Invalid bucket name in gs path: 'gs://a/b'");
  }

  @Test
  public void testGsPathExtractionBucketTooLong() {
    final String bucketName = StringUtils.rightPad("abc", 250, 'x');
    final String pathName = String.format("gs://%s/path", bucketName);
    assertThatThrownBy(() -> GcsPdao.getGcsLocatorFromGsPath(pathName))
        .hasMessage(String.format("Invalid bucket name in gs path: '%s'", pathName));
  }

  @Test
  public void testGsPathExtractionBucketComponentTooLong() {
    final String bucketComponent = StringUtils.rightPad("a", 64, 'x');
    final String pathName = String.format("gs://abc.%s/path", bucketComponent);
    assertThatThrownBy(() -> GcsPdao.getGcsLocatorFromGsPath(pathName))
        .hasMessage(
            String.format(
                "Component name '%s' too long in gs path: '%s'", bucketComponent, pathName));
  }
}
