package bio.terra.service.filedata.azure.util;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import bio.terra.common.category.Unit;
import com.azure.storage.blob.BlobUrlParts;
import java.util.Locale;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.test.context.ActiveProfiles;

@RunWith(MockitoJUnitRunner.class)
@ActiveProfiles({"google", "unittest"})
@Category(Unit.class)
public class ContainerSasTokenSasUrlFactoryTest {

  private ContainerSasTokenSasUrlFactory sasUrlFactory;
  private static final String SIGNATURE_VALUE = "123";
  private static final String PERMISSIONS_VALUE = "r";
  private static final String EXPIRY_VALUE = "2015-07-02T08:49Z";

  @Before
  public void setUp() {
    BlobUrlParts containerUrl =
        BlobUrlParts.parse(
            String.format(
                Locale.ROOT,
                "https://mydata.blob.core.windows.net/cont/?sig=%s&sp=%s&se=%s",
                SIGNATURE_VALUE,
                PERMISSIONS_VALUE,
                EXPIRY_VALUE));
    sasUrlFactory = new ContainerSasTokenSasUrlFactory(containerUrl);
  }

  @Test
  public void testBlobSasTokenContainsSignatureFromContainerUrl() {

    String sasToken = sasUrlFactory.createSasUrlForBlob("myblob", null);
    assertThat(
        BlobUrlParts.parse(sasToken).getCommonSasQueryParameters().getSignature(),
        equalTo(SIGNATURE_VALUE));
  }

  @Test
  public void testBlobSasTokenContainsPermissionsFromContainerUrl() {

    String sasToken = sasUrlFactory.createSasUrlForBlob("myblob", null);
    assertThat(
        BlobUrlParts.parse(sasToken).getCommonSasQueryParameters().getPermissions(),
        equalTo(PERMISSIONS_VALUE));
  }

  @Test
  public void testBlobSasTokenContainsExpirationFromContainerUrl() {

    String sasToken = sasUrlFactory.createSasUrlForBlob("myblob", null);
    assertThat(
        BlobUrlParts.parse(sasToken).getCommonSasQueryParameters().getExpiryTime().toString(),
        equalTo(EXPIRY_VALUE));
  }
}
