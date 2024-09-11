package bio.terra.service.resourcemanagement.google;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import bio.terra.app.model.GoogleRegion;
import bio.terra.common.category.Unit;
import bio.terra.service.filedata.google.gcs.GcsProject;
import bio.terra.service.filedata.google.gcs.GcsProjectFactory;
import com.google.cloud.Policy;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.BucketInfo.Autoclass;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageClass;
import java.time.Duration;
import org.junit.jupiter.api.BeforeEach;
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

  private static final String BUCKET_NAME = "bucket";
  private static final String PROJECT_ID = "project";

  @Mock private GoogleResourceDao googleResourceDao;
  @Mock private GcsProjectFactory gcsProjectFactory;
  @Mock private Environment environment;

  private GoogleBucketService googleBucketService;

  @BeforeEach
  void setUp() {
    googleBucketService =
        new GoogleBucketService(googleResourceDao, gcsProjectFactory, null, environment);
  }

  Storage mockStorageForService() {
    GcsProject gcsProject = mock(GcsProject.class);
    when(gcsProjectFactory.get(PROJECT_ID, true)).thenReturn(gcsProject);
    Storage storage = mock(Storage.class);
    when(gcsProject.getStorage()).thenReturn(storage);
    return storage;
  }

  @Test
  void testGetStorageForBucketResource() {
    GoogleBucketResource bucketResource =
        new GoogleBucketResource()
            .name(BUCKET_NAME)
            .region(GoogleRegion.DEFAULT_GOOGLE_REGION)
            .projectResource(new GoogleProjectResource().googleProjectId(PROJECT_ID));
    Storage storage = mockStorageForService();
    Storage actual = googleBucketService.getStorageForBucketResource(bucketResource);
    assertThat(actual, is(storage));
  }

  @Test
  void testGetBucketMetadata() {
    GoogleBucketResource expected =
        new GoogleBucketResource()
            .name(BUCKET_NAME)
            .region(GoogleRegion.DEFAULT_GOOGLE_REGION)
            .projectResource(new GoogleProjectResource().googleProjectId(PROJECT_ID));

    when(googleResourceDao.retrieveBucketByName(BUCKET_NAME)).thenReturn(expected);

    GoogleBucketResource actual = googleBucketService.getBucketMetadata(BUCKET_NAME);
    assertThat(actual, is(expected));
  }

  @Test
  void testNewCloudBucket() {
    GoogleBucketResource resource =
        new GoogleBucketResource()
            .name(BUCKET_NAME)
            .region(GoogleRegion.DEFAULT_GOOGLE_REGION)
            .projectResource(new GoogleProjectResource().googleProjectId(PROJECT_ID));

    when(environment.getActiveProfiles()).thenReturn(new String[] {});
    ArgumentCaptor<BucketInfo> bucketInfo = ArgumentCaptor.forClass(BucketInfo.class);
    Storage storage = mockStorageForService();
    Bucket expected = mock(Bucket.class);
    when(expected.getName()).thenReturn(BUCKET_NAME);
    when(storage.create(bucketInfo.capture())).thenReturn(expected);
    when(storage.getIamPolicy(BUCKET_NAME, Storage.BucketSourceOption.requestedPolicyVersion(3)))
        .thenReturn(Policy.newBuilder().build());

    Bucket actual =
        googleBucketService.newCloudBucket(
            resource, Duration.ofDays(10), null, "service account", true);
    assertThat(actual, is(expected));
    assertThat(
        bucketInfo.getValue().getAutoclass().getTerminalStorageClass(), is(StorageClass.ARCHIVE));
  }

  @Test
  void testSetBucketAutoclassEnable() {
    // Set up the bucket resources
    boolean enableAutoclass = true;
    StorageClass storageClass = StorageClass.ARCHIVE;
    GoogleBucketResource bucketResource =
        new GoogleBucketResource()
            .name(BUCKET_NAME)
            .region(GoogleRegion.DEFAULT_GOOGLE_REGION)
            .projectResource(new GoogleProjectResource().googleProjectId(PROJECT_ID))
            .autoclassEnabled(enableAutoclass);
    Autoclass autoclassSetting =
        Autoclass.newBuilder()
            .setEnabled(enableAutoclass)
            .setTerminalStorageClass(storageClass)
            .build();

    // Mock the storage and bucket
    Storage storage = mockStorageForService();
    Bucket expected = mock(Bucket.class);
    when(storage.get(BUCKET_NAME)).thenReturn(expected);
    Bucket.Builder bucketBuilder = mock(Bucket.Builder.class);
    when(expected.toBuilder()).thenReturn(bucketBuilder);
    when(bucketBuilder.setAutoclass(autoclassSetting)).thenReturn(bucketBuilder);
    when(bucketBuilder.build()).thenReturn(expected);
    when(storage.update(expected)).thenReturn(expected);
    when(expected.getAutoclass()).thenReturn(autoclassSetting);

    // Check that the bucket has the correct autoclass setting
    Bucket actual =
        googleBucketService.setBucketAutoclass(
            bucketResource, enableAutoclass, StorageClass.ARCHIVE);
    assertThat(actual, is(expected));
    assertThat(actual.getAutoclass(), is(autoclassSetting));
    assertThat(actual.getAutoclass().getTerminalStorageClass(), is(storageClass));
  }

  @Test
  void testSetBucketAutoclassDisable() {
    // Set up the bucket resources
    boolean enableAutoclass = false;
    GoogleBucketResource bucketResource =
        new GoogleBucketResource()
            .name(BUCKET_NAME)
            .region(GoogleRegion.DEFAULT_GOOGLE_REGION)
            .projectResource(new GoogleProjectResource().googleProjectId(PROJECT_ID))
            .autoclassEnabled(enableAutoclass);
    Autoclass autoclassSetting = Autoclass.newBuilder().setEnabled(enableAutoclass).build();

    // Mock the storage and bucket
    Storage storage = mockStorageForService();
    Bucket expected = mock(Bucket.class);
    when(storage.get(BUCKET_NAME)).thenReturn(expected);
    Bucket.Builder bucketBuilder = mock(Bucket.Builder.class);
    when(expected.toBuilder()).thenReturn(bucketBuilder);
    when(bucketBuilder.setAutoclass(autoclassSetting)).thenReturn(bucketBuilder);
    when(bucketBuilder.build()).thenReturn(expected);
    when(storage.update(expected)).thenReturn(expected);
    when(expected.getAutoclass()).thenReturn(autoclassSetting);

    // Check that the bucket has no autoclass setting
    Bucket actual =
        googleBucketService.setBucketAutoclass(
            bucketResource, enableAutoclass, StorageClass.ARCHIVE);
    assertThat(actual, is(expected));
    assertThat(actual.getAutoclass(), is(autoclassSetting));
    assertThat(actual.getAutoclass().getTerminalStorageClass(), nullValue());
  }

  @Test
  void testSetBucketAutoclassToArchive() {
    // Set up the bucket resources
    boolean enableAutoclass = true;
    StorageClass storageClass = StorageClass.ARCHIVE;
    GoogleBucketResource bucketResource =
        new GoogleBucketResource()
            .name(BUCKET_NAME)
            .region(GoogleRegion.DEFAULT_GOOGLE_REGION)
            .projectResource(new GoogleProjectResource().googleProjectId(PROJECT_ID))
            .autoclassEnabled(enableAutoclass);
    Autoclass autoclassSetting =
        Autoclass.newBuilder()
            .setEnabled(enableAutoclass)
            .setTerminalStorageClass(storageClass)
            .build();

    // Mock the storage and bucket
    Storage storage = mockStorageForService();
    Bucket expected = mock(Bucket.class);
    when(storage.get(BUCKET_NAME)).thenReturn(expected);
    Bucket.Builder bucketBuilder = mock(Bucket.Builder.class);
    when(expected.toBuilder()).thenReturn(bucketBuilder);
    when(bucketBuilder.setAutoclass(autoclassSetting)).thenReturn(bucketBuilder);
    when(bucketBuilder.build()).thenReturn(expected);
    when(storage.update(expected)).thenReturn(expected);
    when(expected.getAutoclass()).thenReturn(autoclassSetting);

    // Check that the bucket has the correct autoclass setting
    Bucket actual = googleBucketService.setBucketAutoclassToArchive(bucketResource);
    assertThat(actual, is(expected));
    assertThat(actual.getAutoclass(), is(autoclassSetting));
    assertThat(actual.getAutoclass().getTerminalStorageClass(), is(storageClass));
  }
}
