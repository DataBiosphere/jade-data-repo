package bio.terra.service.resourcemanagement.google;

import bio.terra.common.category.Unit;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.test.context.junit4.SpringRunner;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

@RunWith(SpringRunner.class)
@Category(Unit.class)
public class GoogleProjectServiceTest {

    @Test
    public void testVerifyProjectId() {
        // Should pass
        GoogleProjectService.validateProjectId("abc1234-567");
        // All below should fail

        assertThatThrownBy(
            () -> GoogleProjectService.validateProjectId("abc1234_567"),
            "Can only contain letters, numbers, and hyphens")
                .hasMessage(
                    "The project ID \"abc1234_567\" must be a unique string of 6 to 30 lowercase letters, digits, " +
                    "or hyphens. It must start with a letter, and cannot have a trailing hyphen. You cannot change a " +
                    "project ID once it has been created. You cannot re-use a project ID that is in use, or one that " +
                    "has been used for a deleted project.");

        assertThatThrownBy(
            () -> GoogleProjectService.validateProjectId("aBc1234-567"),
            "Can't have uppercase letters");

        assertThatThrownBy(
            () -> GoogleProjectService.validateProjectId("1bc1234-567"),
            "Can't start with anything but a letter");
        assertThatThrownBy(
            () -> GoogleProjectService.validateProjectId("-bc1234-567"),
            "Can't start with anything but a letter");

        assertThatThrownBy(
            () -> GoogleProjectService.validateProjectId("abc12"),
            "Can't contain fewer than 6 characters");
        GoogleProjectService.validateProjectId("abc123");

        assertThatThrownBy(
            () -> GoogleProjectService.validateProjectId("a012345678901234567890123456789"),
            "Can't contain more than 30 characters");
        GoogleProjectService.validateProjectId("a01234567890123456789012345678");

        assertThatThrownBy(
            () -> GoogleProjectService.validateProjectId("abc1234-567-"),
            "Can't end with a hyphen");
    }
}
