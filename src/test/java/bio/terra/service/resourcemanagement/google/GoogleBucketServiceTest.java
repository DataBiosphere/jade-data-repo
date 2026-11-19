package bio.terra.service.resourcemanagement.google;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import bio.terra.app.model.GoogleRegion;
import bio.terra.common.category.Unit;
import bio.terra.service.filedata.google.gcs.GcsProject;
import bio.terra.service.filedata.google.gcs.GcsProjectFactory;
import com.google.cloud.Binding;
import com.google.cloud.Policy;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.BucketInfo.Autoclass;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageClass;
import com.google.cloud.storage.StorageException;
import java.time.Duration;
import java.util.List;
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
  void testEnsureObjectViewerAddsMissingBinding() {
    String member = "serviceAccount:tdr@project.iam.gserviceaccount.com";
    Storage storage = mockStorageForService();
    Binding existing =
        Binding.newBuilder()
            .setRole("roles/storage.legacyBucketOwner")
            .setMembers(List.of("projectOwner:project"))
            .build();
    when(storage.getIamPolicy(eq(BUCKET_NAME), any()))
        .thenReturn(Policy.newBuilder().setBindings(List.of(existing)).build());

    googleBucketService.ensureObjectViewer(bucketResource(), "tdr@project.iam.gserviceaccount.com");

    ArgumentCaptor<Policy> policy = ArgumentCaptor.forClass(Policy.class);
    verify(storage).setIamPolicy(eq(BUCKET_NAME), policy.capture());
    assertThat(policy.getValue().getBindingsList(), hasItem(existing));
    assertThat(
        policy.getValue().getBindingsList(),
        hasItem(
            Binding.newBuilder()
                .setRole(GoogleBucketService.STORAGE_OBJECT_VIEWER_ROLE)
                .setMembers(List.of(member))
                .build()));
  }

  @Test
  void testEnsureObjectViewerLeavesExistingBindingAlone() {
    Storage storage = mockStorageForService();
    Binding viewer =
        Binding.newBuilder()
            .setRole(GoogleBucketService.STORAGE_OBJECT_VIEWER_ROLE)
            .setMembers(
                List.of(
                    "group:readers@example.com",
                    "serviceAccount:tdr@project.iam.gserviceaccount.com"))
            .build();
    when(storage.getIamPolicy(eq(BUCKET_NAME), any()))
        .thenReturn(Policy.newBuilder().setBindings(List.of(viewer)).build());

    googleBucketService.ensureObjectViewer(bucketResource(), "tdr@project.iam.gserviceaccount.com");

    verify(storage, never()).setIamPolicy(any(), any());
  }

  @Test
  void testEnsureObjectViewerRetriesConcurrentPolicyChange() {
    Storage storage = mockStorageForService();
    when(storage.getIamPolicy(eq(BUCKET_NAME), any())).thenReturn(Policy.newBuilder().build());
    when(storage.setIamPolicy(eq(BUCKET_NAME), any()))
        .thenThrow(new StorageException(412, "Precondition Failed"))
        .thenReturn(Policy.newBuilder().build());

    googleBucketService.ensureObjectViewer(bucketResource(), "tdr@project.iam.gserviceaccount.com");

    verify(storage, times(2)).getIamPolicy(eq(BUCKET_NAME), any());
    verify(storage, times(2)).setIamPolicy(eq(BUCKET_NAME), any());
  }

  private GoogleBucketResource bucketResource() {
    return new GoogleBucketResource()
        .name(BUCKET_NAME)
        .region(GoogleRegion.DEFAULT_GOOGLE_REGION)
        .projectResource(new GoogleProjectResource().googleProjectId(PROJECT_ID));
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
    StorageClass storageClass = StorageClass.STANDARD;
    StorageClass terminalStorageClass = StorageClass.ARCHIVE;
    GoogleBucketResource bucketResource =
        new GoogleBucketResource()
            .name(BUCKET_NAME)
            .region(GoogleRegion.DEFAULT_GOOGLE_REGION)
            .projectResource(new GoogleProjectResource().googleProjectId(PROJECT_ID))
            .autoclassEnabled(enableAutoclass)
            .storageClass(storageClass);
    Autoclass autoclassSetting =
        Autoclass.newBuilder()
            .setEnabled(enableAutoclass)
            .setTerminalStorageClass(terminalStorageClass)
            .build();

    // Mock the storage and bucket
    Storage storage = mockStorageForService();
    Bucket expected = mock(Bucket.class);
    when(storage.get(BUCKET_NAME)).thenReturn(expected);
    Bucket.Builder bucketBuilder = mock(Bucket.Builder.class);
    when(expected.toBuilder()).thenReturn(bucketBuilder);
    when(bucketBuilder.setStorageClass(storageClass)).thenReturn(bucketBuilder);
    when(bucketBuilder.setAutoclass(autoclassSetting)).thenReturn(bucketBuilder);
    when(bucketBuilder.build()).thenReturn(expected);
    when(storage.update(expected)).thenReturn(expected);
    when(expected.getAutoclass()).thenReturn(autoclassSetting);

    // Check that the bucket has the correct autoclass setting
    Bucket actual =
        googleBucketService.setBucketAutoclass(
            bucketResource, enableAutoclass, storageClass, terminalStorageClass);
    assertThat(actual, is(expected));
    assertThat(actual.getAutoclass(), is(autoclassSetting));
    assertThat(actual.getAutoclass().getTerminalStorageClass(), is(terminalStorageClass));
  }

  @Test
  void testSetBucketAutoclassDisable() {
    // Set up the bucket resources
    boolean enableAutoclass = false;
    StorageClass storageClass = StorageClass.STANDARD;
    GoogleBucketResource bucketResource =
        new GoogleBucketResource()
            .name(BUCKET_NAME)
            .region(GoogleRegion.DEFAULT_GOOGLE_REGION)
            .projectResource(new GoogleProjectResource().googleProjectId(PROJECT_ID))
            .autoclassEnabled(enableAutoclass)
            .storageClass(storageClass);
    Autoclass autoclassSetting = Autoclass.newBuilder().setEnabled(enableAutoclass).build();

    // Mock the storage and bucket
    Storage storage = mockStorageForService();
    Bucket expected = mock(Bucket.class);
    when(storage.get(BUCKET_NAME)).thenReturn(expected);
    Bucket.Builder bucketBuilder = mock(Bucket.Builder.class);
    when(expected.toBuilder()).thenReturn(bucketBuilder);
    when(bucketBuilder.setStorageClass(storageClass)).thenReturn(bucketBuilder);
    when(bucketBuilder.setAutoclass(autoclassSetting)).thenReturn(bucketBuilder);
    when(bucketBuilder.build()).thenReturn(expected);
    when(storage.update(expected)).thenReturn(expected);
    when(expected.getAutoclass()).thenReturn(autoclassSetting);

    // Check that the bucket has no autoclass setting
    Bucket actual =
        googleBucketService.setBucketAutoclass(
            bucketResource, enableAutoclass, storageClass, StorageClass.ARCHIVE);
    assertThat(actual, is(expected));
    assertThat(actual.getAutoclass(), is(autoclassSetting));
    assertThat(actual.getAutoclass().getTerminalStorageClass(), nullValue());
  }

  @Test
  void testSetBucketAutoclassToArchive() {
    // Set up the bucket resources
    boolean enableAutoclass = true;
    StorageClass storageClass = StorageClass.STANDARD;
    StorageClass terminalStorageClass = StorageClass.ARCHIVE;
    GoogleBucketResource bucketResource =
        new GoogleBucketResource()
            .name(BUCKET_NAME)
            .region(GoogleRegion.DEFAULT_GOOGLE_REGION)
            .projectResource(new GoogleProjectResource().googleProjectId(PROJECT_ID))
            .autoclassEnabled(enableAutoclass);
    Autoclass autoclassSetting =
        Autoclass.newBuilder()
            .setEnabled(enableAutoclass)
            .setTerminalStorageClass(terminalStorageClass)
            .build();

    // Mock the storage and bucket
    Storage storage = mockStorageForService();
    Bucket expected = mock(Bucket.class);
    when(storage.get(BUCKET_NAME)).thenReturn(expected);
    Bucket.Builder bucketBuilder = mock(Bucket.Builder.class);
    when(expected.toBuilder()).thenReturn(bucketBuilder);
    when(bucketBuilder.setStorageClass(storageClass)).thenReturn(bucketBuilder);
    when(bucketBuilder.setAutoclass(autoclassSetting)).thenReturn(bucketBuilder);
    when(bucketBuilder.build()).thenReturn(expected);
    when(storage.update(expected)).thenReturn(expected);
    when(expected.getAutoclass()).thenReturn(autoclassSetting);

    // Check that the bucket has the correct autoclass setting
    Bucket actual = googleBucketService.setBucketAutoclassToArchive(bucketResource);
    assertThat(actual, is(expected));
    assertThat(actual.getAutoclass(), is(autoclassSetting));
    assertThat(actual.getAutoclass().getTerminalStorageClass(), is(terminalStorageClass));
  }

  @Test
  void testSetBucketAutoclassMetadata() {
    when(googleResourceDao.updateBucketAutoclassByName(BUCKET_NAME, true)).thenReturn(1);
    int rows = googleBucketService.setBucketAutoclassMetadata(BUCKET_NAME, true);
    assertThat(rows, is(1));
  }
}
