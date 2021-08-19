package bio.terra.service.filedata.google.firestore;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;

import bio.terra.common.category.Unit;
import bio.terra.model.CloudPlatform;
import bio.terra.service.filedata.FSFile;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(Unit.class)
public class FSFileTest {

  @Test
  public void testCloudPlatformDetection() {
    assertThat(
        new FSFile().cloudPath("gs://mybucket/test.txt").getCloudPlatform(),
        equalTo(CloudPlatform.GCP));
    assertThat(
        new FSFile()
            .cloudPath("gs://mybucket/dsid/fid/file with space and #hash%percent+plus.txt")
            .getCloudPlatform(),
        equalTo(CloudPlatform.GCP));
    assertThat(
        new FSFile()
            .cloudPath("https://myacct.blob.core.windows.net/fs/test.txt")
            .getCloudPlatform(),
        equalTo(CloudPlatform.AZURE));
    assertThrows(
        IllegalArgumentException.class,
        () -> new FSFile().cloudPath("ftp://notsupported.txt").getCloudPlatform());
    assertThrows(
        IllegalArgumentException.class,
        () -> new FSFile().cloudPath("https://myhost.com/notsupported.txt").getCloudPlatform());
  }
}
