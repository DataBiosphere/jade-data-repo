package bio.terra.service.filedata.azure.util;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.mockito.Answers.CALLS_REAL_METHODS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.sas.BlobSasPermission;
import com.azure.storage.blob.sas.BlobServiceSasSignatureValues;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.test.context.ActiveProfiles;

@RunWith(MockitoJUnitRunner.class)
@ActiveProfiles({"google", "unittest"})
@Category(Unit.class)
public class KeySasUrlFactoryTest {
  private static final String SAS_TOKEN = "sig=123&sv=2020-06-10&sp=crwl";
  private static final String CONTENT_DISPOSITION = "myuser@xyz.org";
  private BlobSasTokenOptions sasTokenOptions;
  private BlobSasPermission permissions;

  @Mock private BlobContainerClient blobContainerClient;

  @Mock private BlobClient blobClient;

  private KeySasUrlFactory keySasUrlFactory;

  @Captor ArgumentCaptor<BlobServiceSasSignatureValues> signatureValuesArgumentCaptor;

  @Before
  public void setUp() {
    when(blobContainerClient.getBlobClient(any())).thenReturn(blobClient);

    keySasUrlFactory =
        mock(
            KeySasUrlFactory.class,
            Mockito.withSettings()
                .useConstructor(blobContainerClient)
                .defaultAnswer(CALLS_REAL_METHODS));

    when(keySasUrlFactory.generateSasToken(signatureValuesArgumentCaptor.capture(), any()))
        .thenReturn(SAS_TOKEN);
    Duration expiration = Duration.ofMinutes(15);
    permissions = new BlobSasPermission().setReadPermission(true);
    sasTokenOptions = new BlobSasTokenOptions(expiration, permissions, CONTENT_DISPOSITION);
  }

  @Test
  public void testContentDispositionWhenSetInOptions() {
    keySasUrlFactory.createSasUrlForBlob("myblob", sasTokenOptions);

    assertThat(
        signatureValuesArgumentCaptor.getValue().getContentDisposition(),
        equalTo(CONTENT_DISPOSITION));
  }

  @Test
  public void testPermissionsWhenSetInOptions() {
    keySasUrlFactory.createSasUrlForBlob("myblob", sasTokenOptions);

    assertThat(
        signatureValuesArgumentCaptor.getValue().getPermissions(), equalTo(permissions.toString()));
  }

  @Test
  public void testExpirationWhenSetInOptions() {
    keySasUrlFactory.createSasUrlForBlob("myblob", sasTokenOptions);

    assertThat(
        signatureValuesArgumentCaptor.getValue().getExpiryTime(),
        greaterThan(OffsetDateTime.now(ZoneOffset.UTC)));
  }
}
