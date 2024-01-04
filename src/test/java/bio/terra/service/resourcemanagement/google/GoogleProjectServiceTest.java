package bio.terra.service.resourcemanagement.google;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;

import bio.terra.common.category.Unit;
import bio.terra.service.resourcemanagement.exception.AppengineException;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.test.context.ActiveProfiles;

@ActiveProfiles({"google", "unittest"})
@Tag(Unit.TAG)
class GoogleProjectServiceTest {

  private static final UUID RANDOM_UUID = UUID.randomUUID();
  private static final String APP_ID = "my-project";

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
                APP_ID, String.format("apps/%s/operations/%s", APP_ID, opId)))
        .as("works as expected")
        .isEqualTo(opId);
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
}
