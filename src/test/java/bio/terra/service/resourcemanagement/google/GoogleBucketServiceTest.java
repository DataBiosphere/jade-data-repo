package bio.terra.service.resourcemanagement.google;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import bio.terra.app.model.GoogleRegion;
import bio.terra.common.category.Unit;
import bio.terra.service.filedata.google.gcs.GcsProject;
import bio.terra.service.filedata.google.gcs.GcsProjectFactory;
import com.google.cloud.Policy;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageClass;
import java.time.Duration;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.core.env.Environment;

@Tag(Unit.TAG)
@ExtendWith(MockitoExtension.class)
public class GoogleBucketServiceTest {
  @Mock private GcsProjectFactory gcsProjectFactory;
  @Mock private Environment environment;

  @Test
  void newCloudBucket() {
    GoogleBucketResource resource =
        new GoogleBucketResource()
            .name("bucket")
            .region(GoogleRegion.DEFAULT_GOOGLE_REGION)
            .projectResource(new GoogleProjectResource().googleProjectId("project"));
    when(environment.getActiveProfiles()).thenReturn(new String[] {});
    GcsProject gcsProject = mock(GcsProject.class);
    when(gcsProjectFactory.get("project", true)).thenReturn(gcsProject);
    Storage storage = mock(Storage.class);
    when(gcsProject.getStorage()).thenReturn(storage);
    ArgumentCaptor<BucketInfo> bucketInfo = ArgumentCaptor.forClass(BucketInfo.class);
    Bucket expected = mock(Bucket.class);
    when(expected.getName()).thenReturn("bucket");
    when(storage.create(bucketInfo.capture())).thenReturn(expected);
    when(storage.getIamPolicy("bucket", Storage.BucketSourceOption.requestedPolicyVersion(3)))
        .thenReturn(Policy.newBuilder().build());

    GoogleBucketService service =
        new GoogleBucketService(null, gcsProjectFactory, null, environment);
    Bucket actual =
        service.newCloudBucket(resource, Duration.ofDays(10), null, "service account", true);
    assertThat(actual, is(expected));
    assertThat(
        bucketInfo.getValue().getAutoclass().getTerminalStorageClass(), is(StorageClass.ARCHIVE));
  }
}
