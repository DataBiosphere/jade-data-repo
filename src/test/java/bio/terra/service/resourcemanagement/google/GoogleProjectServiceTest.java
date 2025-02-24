package bio.terra.service.resourcemanagement.google;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasLength;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

import bio.terra.common.category.Unit;
import bio.terra.service.resourcemanagement.exception.AppengineException;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@Tag(Unit.TAG)
class GoogleProjectServiceTest {

  private static final UUID RANDOM_UUID = UUID.randomUUID();
  private static final String APP_ID = "my-project";

  @Test
  void testVerifyProjectId() {
    // Should pass
    GoogleProjectService.ensureValidProjectId("abc1234-567");
    // All below should fail
    Exception e;

    e =
        assertThrows(
            NullPointerException.class,
            () -> GoogleProjectService.ensureValidProjectId(null),
            "Can't be null");
    assertThat(e.getMessage(), is("Project Id must not be null"));

    e =
        assertThrows(
            IllegalArgumentException.class,
            () -> GoogleProjectService.ensureValidProjectId("abc1234_567"),
            "Can only contain letters, numbers, and hyphens");
    assertThat(
        e.getMessage(),
        is(
            "The project ID \"abc1234_567\" must be a unique string of 6 to 30 lowercase letters, digits, "
                + "or hyphens. It must start with a letter, and cannot have a trailing hyphen. You cannot change a "
                + "project ID once it has been created. You cannot re-use a project ID that is in use, or one that "
                + "has been used for a deleted project."));

    assertThrows(
        IllegalArgumentException.class,
        () -> GoogleProjectService.ensureValidProjectId("aBc1234-567"),
        "Can't have uppercase letters");

    assertThrows(
        IllegalArgumentException.class,
        () -> GoogleProjectService.ensureValidProjectId("1bc1234-567"),
        "Can't start with anything but a letter");

    assertThrows(
        IllegalArgumentException.class,
        () -> GoogleProjectService.ensureValidProjectId("-bc1234-567"),
        "Can't start with anything but a letter");

    assertThrows(
        IllegalArgumentException.class,
        () -> GoogleProjectService.ensureValidProjectId("abc12"),
        "Can't contain fewer than 6 characters");
    // Check that 6 characters is OK
    GoogleProjectService.ensureValidProjectId("abc123");

    assertThrows(
        IllegalArgumentException.class,
        () -> GoogleProjectService.ensureValidProjectId("a012345678901234567890123456789"),
        "Can't contain more than 30 characters");
    // Check that 30 characters is OK
    GoogleProjectService.ensureValidProjectId("a01234567890123456789012345678");

    assertThrows(
        IllegalArgumentException.class,
        () -> GoogleProjectService.ensureValidProjectId("abc1234-567-"),
        "Can't end with a hyphen");
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
        "works as expected",
        GoogleProjectService.extractOperationIdFromName(
            APP_ID, String.format("apps/%s/operations/%s", APP_ID, opId)),
        is(opId));
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
    var e =
        assertThrows(
            AppengineException.class,
            () -> GoogleProjectService.extractOperationIdFromName(APP_ID, prefix + RANDOM_UUID),
            "handles unexpected prefix");
    assertThat(e.getMessage(), containsString("does not start with expected prefix"));
  }

  @Test
  void extractOperationIdFromName_unexpectedElementCount() {
    String opName = String.format("apps/%s/operations/%s/extraelement", APP_ID, RANDOM_UUID);
    var e =
        assertThrows(
            AppengineException.class,
            () -> GoogleProjectService.extractOperationIdFromName(APP_ID, opName),
            "handles unexpected element count");
    assertThat(e.getMessage(), containsString("expected to have exactly 4 elements"));
  }

  @Test
  void testProjectLabelClean() {
    String tooLongName = "workflow_launcher_testing_dataset5243fe12db16406789e76e98dcf3aebd";
    assertThat("Project label original length should be 65", tooLongName, hasLength(65));
    String trimmedName = GoogleResourceManagerService.cleanForLabels(tooLongName);
    assertThat("Project label should be trimmed down when too long", trimmedName, hasLength(63));

    String nameWithCharacters = "workflow!_launcher+TESTING_dataset5243fe12db1640";
    String expectedCleanedName = "workflow-_launcher-testing_dataset5243fe12db1640";
    String cleanedName = GoogleResourceManagerService.cleanForLabels(nameWithCharacters);
    assertThat(
        "Original Project label should no longer contain non-valid characters",
        cleanedName,
        is(expectedCleanedName));
  }
}
