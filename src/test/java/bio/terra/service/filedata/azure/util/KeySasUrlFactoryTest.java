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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@Tag(Unit.TAG)
class KeySasUrlFactoryTest {
  private static final String SAS_TOKEN = "sig=123&sv=2020-06-10&sp=crwl";
  private static final String CONTENT_DISPOSITION = "myuser@xyz.org";
  private BlobSasTokenOptions sasTokenOptions;
  private BlobSasPermission permissions;

  @Mock private BlobContainerClient blobContainerClient;

  @Mock private BlobClient blobClient;

  private KeySasUrlFactory keySasUrlFactory;

  @Captor ArgumentCaptor<BlobServiceSasSignatureValues> signatureValuesArgumentCaptor;

  @BeforeEach
  void setUp() {
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
  void testContentDispositionWhenSetInOptions() {
    keySasUrlFactory.createSasUrlForBlob("myblob", sasTokenOptions);

    assertThat(
        signatureValuesArgumentCaptor.getValue().getContentDisposition(),
        equalTo(CONTENT_DISPOSITION));
  }

  @Test
  void testPermissionsWhenSetInOptions() {
    keySasUrlFactory.createSasUrlForBlob("myblob", sasTokenOptions);

    assertThat(
        signatureValuesArgumentCaptor.getValue().getPermissions(), equalTo(permissions.toString()));
  }

  @Test
  void testExpirationWhenSetInOptions() {
    keySasUrlFactory.createSasUrlForBlob("myblob", sasTokenOptions);

    assertThat(
        signatureValuesArgumentCaptor.getValue().getExpiryTime(),
        greaterThan(OffsetDateTime.now(ZoneOffset.UTC)));
  }
}
