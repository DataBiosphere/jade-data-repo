package bio.terra.service.resourcemanagement.google;

import bio.terra.common.category.Unit;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

@RunWith(SpringRunner.class)
@AutoConfigureMockMvc
@SpringBootTest
@Category(Unit.class)
public class GoogleProjectServiceTest {

    @Autowired
    private GoogleProjectService projectService;

    @Test
    public void testVerifyProjectId() {
        // Should pass
        GoogleProjectService.ensureValidProjectId("abc1234-567");
        // All below should fail

        assertThatThrownBy(
            () -> GoogleProjectService.ensureValidProjectId(null),
            "Can't be null")
                .hasMessage("Project Id must not be null");

        assertThatThrownBy(
            () -> GoogleProjectService.ensureValidProjectId("abc1234_567"),
            "Can only contain letters, numbers, and hyphens")
                .hasMessage(
                    "The project ID \"abc1234_567\" must be a unique string of 6 to 30 lowercase letters, digits, " +
                    "or hyphens. It must start with a letter, and cannot have a trailing hyphen. You cannot change a " +
                    "project ID once it has been created. You cannot re-use a project ID that is in use, or one that " +
                    "has been used for a deleted project.");

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
            () -> GoogleProjectService.ensureValidProjectId("abc1234-567-"),
            "Can't end with a hyphen");
    }


    @Test
    @Ignore("Un-ignore to test the explicit activation of a Firestore DB in an empty project")
    public void testInitFirestore() throws InterruptedException {
        projectService.enableServices(new GoogleProjectResource()
            .googleProjectId("")
            .googleProjectNumber(""));
    }
}
