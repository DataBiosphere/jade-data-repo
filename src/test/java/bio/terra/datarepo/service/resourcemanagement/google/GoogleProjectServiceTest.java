package bio.terra.datarepo.service.resourcemanagement.google;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import bio.terra.datarepo.common.category.Unit;
import bio.terra.datarepo.service.resourcemanagement.exception.AppengineException;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@Category(Unit.class)
public class GoogleProjectServiceTest {

  @Test
  public void testVerifyProjectId() {
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

  @Test
  public void testAppEngineOpIdExtraction() {
    assertThat(
            GoogleProjectService.extractOperationIdFromName(
                "my-project", "apps/my-project/operations/aa9a20e4-69a9-488d-978f-0c55cc2beae8"))
        .as("works as expected")
        .isEqualTo("aa9a20e4-69a9-488d-978f-0c55cc2beae8");

    assertThatThrownBy(
            () ->
                GoogleProjectService.extractOperationIdFromName(
                    "my-project",
                    "apps/my-project/somenewformat/aa9a20e4-69a9-488d-978f-0c55cc2beae8"))
        .as("handles bad path")
        .isInstanceOf(AppengineException.class)
        .hasMessageStartingWith("Operation Name does not look as expected");

    assertThatThrownBy(
            () ->
                GoogleProjectService.extractOperationIdFromName(
                    "my-project",
                    "apps/my-project/operations/aa9a20g4-69a9-488d-978f-0c55cc2beae8"))
        .as("handles bad uuid")
        .isInstanceOf(AppengineException.class)
        .hasMessageStartingWith("Operation Name does not look as expected");
  }
}
