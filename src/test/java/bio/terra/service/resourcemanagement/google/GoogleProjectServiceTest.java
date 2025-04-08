package bio.terra.service.resourcemanagement.google;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import bio.terra.app.model.GoogleRegion;
import bio.terra.common.CollectionType;
import bio.terra.common.category.Unit;
import bio.terra.service.dataset.DatasetBucketDao;
import bio.terra.service.profile.google.GoogleBillingService;
import bio.terra.service.resourcemanagement.BufferService;
import bio.terra.service.resourcemanagement.exception.AppengineException;
import bio.terra.service.resourcemanagement.exception.GoogleResourceException;
import bio.terra.service.resourcemanagement.exception.GoogleResourceNotFoundException;
import bio.terra.service.resourcemanagement.exception.MismatchedBillingProfilesException;
import com.google.api.services.cloudresourcemanager.model.Project;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@Tag(Unit.TAG)
class GoogleProjectServiceTest {

  private static final UUID RANDOM_UUID = UUID.randomUUID();
  private static final String APP_ID = "my-project";

  private GoogleProjectService googleProjectService;
  @Mock private GoogleResourceDao googleResourceDao;
  @Mock private GoogleResourceManagerService googleResourceManagerService;

  @BeforeEach
  void setup() {
    googleProjectService =
        new GoogleProjectService(
            googleResourceDao,
            mock(GoogleResourceConfiguration.class),
            mock(GoogleBillingService.class),
            googleResourceManagerService,
            mock(BufferService.class),
            mock(DatasetBucketDao.class));
  }

  @Test
  void testVerifyProjectId() {
    // Should pass
    GoogleProjectService.ensureValidProjectId("abc1234-567");
    // All below should fail

    assertThatThrownBy(() -> GoogleProjectService.ensureValidProjectId(null), "Can't be null")
        .hasMessage("Project Id must not be null");

    assertThatThrownBy(
            () -> GoogleProjectService.ensureValidProjectId("abc1234_567"),
            "Can only contain letters, numbers, and hyphens")
        .hasMessage(
            "The project ID \"abc1234_567\" must be a unique string of 6 to 30 lowercase letters, digits, "
                + "or hyphens. It must start with a letter, and cannot have a trailing hyphen. You cannot change a "
                + "project ID once it has been created. You cannot re-use a project ID that is in use, or one that "
                + "has been used for a deleted project.");

    assertThatThrownBy(
        () -> GoogleProjectService.ensureValidProjectId("aBc1234-567"),
        "Can't have uppercase letters");

    assertThatThrownBy(
        () -> GoogleProjectService.ensureValidProjectId("1bc1234-567"),
        "Can't start with anything but a letter");
    assertThatThrownBy(
        () -> GoogleProjectService.ensureValidProjectId("-bc1234-567"),
        "Can't start with anything but a letter");

    assertThatThrownBy(
        () -> GoogleProjectService.ensureValidProjectId("abc12"),
        "Can't contain fewer than 6 characters");
    // Check that 6 characters is OK
    GoogleProjectService.ensureValidProjectId("abc123");

    assertThatThrownBy(
        () -> GoogleProjectService.ensureValidProjectId("a012345678901234567890123456789"),
        "Can't contain more than 30 characters");
    // Check that 30 characters is OK
    GoogleProjectService.ensureValidProjectId("a01234567890123456789012345678");

    assertThatThrownBy(
        () -> GoogleProjectService.ensureValidProjectId("abc1234-567-"), "Can't end with a hyphen");
  }

  private static Stream<Arguments> extractOperationIdFromName_successful() {
    return Stream.of(
        Arguments.arguments(RANDOM_UUID.toString()),
        Arguments.arguments("operation-" + RANDOM_UUID),
        Arguments.arguments("operation-" + RANDOM_UUID.toString().substring(0, 24)));
  }

  @ParameterizedTest
  @MethodSource
  void extractOperationIdFromName_successful(String opId) {
    assertThat(
        GoogleProjectService.extractOperationIdFromName(
            APP_ID, String.format("apps/%s/operations/%s", APP_ID, opId)),
        equalTo(opId));
  }

  private static Stream<Arguments> extractOperationIdFromName_unexpectedPrefix() {
    return Stream.of(
        Arguments.arguments(String.format("apps-subpathmismatch/%s/operations/", APP_ID)),
        Arguments.arguments(String.format("apps/%s-appidmismatch/operations/", APP_ID)),
        Arguments.arguments(String.format("apps/%s/operations-subpathmismatch/", APP_ID)));
  }

  @ParameterizedTest
  @MethodSource
  void extractOperationIdFromName_unexpectedPrefix(String prefix) {
    assertThatThrownBy(
            () -> GoogleProjectService.extractOperationIdFromName(APP_ID, prefix + RANDOM_UUID))
        .as("handles unexpected prefix")
        .isInstanceOf(AppengineException.class)
        .hasMessageContaining("does not start with expected prefix");
  }

  @Test
  void extractOperationIdFromName_unexpectedElementCount() {
    assertThatThrownBy(
            () ->
                GoogleProjectService.extractOperationIdFromName(
                    APP_ID,
                    String.format("apps/%s/operations/extraelement/%s", APP_ID, RANDOM_UUID)))
        .as("handles unexpected element count")
        .isInstanceOf(AppengineException.class)
        .hasMessageContaining("expected to have exactly 4 elements");
  }

  @Test
  void testProjectLabelClean() {
    String tooLongName = "workflow_launcher_testing_dataset5243fe12db16406789e76e98dcf3aebd";
    assertEquals("Project label original length should be 65", tooLongName.length(), 65);
    String trimmedName = GoogleResourceManagerService.cleanForLabels(tooLongName);
    assertEquals("Project label should be trimmed down when too long", trimmedName.length(), 63);

    String nameWithCharacters = "workflow!_launcher+TESTING_dataset5243fe12db1640";
    String expectedCleanedName = "workflow-_launcher-testing_dataset5243fe12db1640";
    String cleanedName = GoogleResourceManagerService.cleanForLabels(nameWithCharacters);
    assertEquals(
        "Original Project label should no longer contain non-valid characters",
        expectedCleanedName,
        cleanedName);
  }

  //  @Test
  //  void initializeGoogleProjectV2() {
  //  }

  @Test
  void checkIfProjectAlreadyExists_projectExists() {
    var projectId = "project123";
    var billingProfileId = UUID.randomUUID();
    var projectResource = new GoogleProjectResource().profileId(billingProfileId);
    when(googleResourceDao.retrieveProjectByGoogleProjectId(projectId)).thenReturn(projectResource);

    assertThat(
        "project resource is returned",
        googleProjectService.checkIfProjectAlreadyExists(projectId, billingProfileId),
        equalTo(projectResource));
  }

  @Test
  void checkIfProjectAlreadyExists_projectExists_mismatchedBilling() {
    var projectId = "project123";
    var requestedBillingProfileId = UUID.randomUUID();
    var existingProjectBillingProfileId = UUID.randomUUID();
    var projectResource = new GoogleProjectResource().profileId(existingProjectBillingProfileId);
    when(googleResourceDao.retrieveProjectByGoogleProjectId(projectId)).thenReturn(projectResource);

    assertThrows(
        MismatchedBillingProfilesException.class,
        () ->
            googleProjectService.checkIfProjectAlreadyExists(projectId, requestedBillingProfileId));
  }

  @Test
  void checkIfProjectAlreadyExists_noProject() {
    var projectId = "project123";
    var billingProfileId = UUID.randomUUID();

    when(googleResourceDao.retrieveProjectByGoogleProjectId(projectId))
        .thenThrow(new GoogleResourceNotFoundException(""));

    assertThat(
        "project resource is returned",
        googleProjectService.checkIfProjectAlreadyExists(projectId, billingProfileId),
        nullValue());
  }

  @Test
  void initializeGoogleProjectV2() throws InterruptedException {
    var projectId = "project123";
    var billingProfileId = UUID.randomUUID();
    Project project = new Project().setProjectId(projectId);
    GoogleProjectResource projectResource = new GoogleProjectResource();

    when(googleResourceDao.retrieveProjectByGoogleProjectId(projectId))
        .thenThrow(new GoogleResourceNotFoundException(""));
    when(googleResourceManagerService.getProject(projectId)).thenReturn(project);
    GoogleProjectService spyGoogleProjectService = Mockito.spy(googleProjectService);
    doReturn(projectResource)
        .when(spyGoogleProjectService)
        .initializeProjectV2(
            project,
            billingProfileId,
            GoogleRegion.DEFAULT_GOOGLE_REGION,
            new HashMap<>(),
            CollectionType.DATASET);

    assertThat(
        "project resource is returned",
        spyGoogleProjectService.initializeGoogleProjectV2(
            projectId,
            billingProfileId,
            GoogleRegion.DEFAULT_GOOGLE_REGION,
            new HashMap<>(),
            CollectionType.DATASET),
        equalTo(projectResource));
  }

  @Test
  void initializeGoogleProjectV2_NoProjectFound() {
    var projectId = "project123";
    var billingProfileId = UUID.randomUUID();

    when(googleResourceDao.retrieveProjectByGoogleProjectId(projectId))
        .thenThrow(new GoogleResourceNotFoundException(""));
    when(googleResourceManagerService.getProject(projectId)).thenReturn(null);

    assertThrows(
        GoogleResourceException.class,
        () ->
            googleProjectService.initializeGoogleProjectV2(
                projectId,
                billingProfileId,
                GoogleRegion.DEFAULT_GOOGLE_REGION,
                new HashMap<>(),
                CollectionType.DATASET));
  }

  @Test
  void initializeProjectV2() throws InterruptedException {
    var projectId = "project123";
    var billingProfileId = UUID.randomUUID();
    Map<String, String> labels = new HashMap<>();
    Long projectNumber = 1234L;
    Project project =
        new Project()
            .setProjectId(projectId)
            .setProjectNumber(projectNumber)
            .setName("TDR Dataset Project");
    GoogleProjectResource projectResource =
        new GoogleProjectResource()
            .googleProjectId(projectId)
            .id(UUID.randomUUID())
            .profileId(billingProfileId)
            .googleProjectNumber(projectNumber.toString());

    GoogleProjectService spyGoogleProjectService = Mockito.spy(googleProjectService);
    doNothing().when(spyGoogleProjectService).enableServices(any(), any());
    doNothing()
        .when(googleResourceManagerService)
        .addLabelsToProject(project.getProjectId(), labels);
    doNothing().when(googleResourceManagerService).addOrEditNameOfProject(any(), any());
    when(googleResourceDao.createProject(any())).thenReturn(projectResource.getId());

    var actualProjectResource =
        spyGoogleProjectService.initializeProjectV2(
            project,
            billingProfileId,
            GoogleRegion.DEFAULT_GOOGLE_REGION,
            labels,
            CollectionType.DATASET);
    assertThat(
        "project resource is returned",
        actualProjectResource.getGoogleProjectId(),
        equalTo(projectResource.getGoogleProjectId()));
  }
}
