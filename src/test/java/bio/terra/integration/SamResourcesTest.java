package bio.terra.integration;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import bio.terra.common.auth.Users;
import bio.terra.common.category.Integration;
import bio.terra.common.configuration.TestConfiguration;
import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.auth.iam.IamRole;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;

@ExtendWith(SpringExtension.class)
@SpringBootTest(classes = IntegrationTestConfiguration.class)
@ActiveProfiles({"google", "integrationtest"})
@Tag(Integration.TAG)
@Execution(ExecutionMode.CONCURRENT)
class SamResourcesTest {
  // Tests to verify that Sam configuration changes are correct and behave as expected.

  private static final Logger logger = LoggerFactory.getLogger(SamResourcesTest.class);

  @Autowired private SamFixtures samFixtures;
  @Autowired private Users users;

  private final ThreadLocal<List<SamFixtures.Resource>> tlResources =
      ThreadLocal.withInitial(ArrayList::new);

  private void addResource(SamFixtures.Resource resource) {
    tlResources.get().add(resource);
  }

  private TestConfiguration.User steward() {
    return testUsers.get().steward();
  }

  private TestConfiguration.User custodian() {
    return testUsers.get().custodian();
  }

  private TestConfiguration.User reader() {
    return testUsers.get().reader();
  }

  private final ThreadLocal<Users.TestUsers> testUsers =
      ThreadLocal.withInitial(() -> users.testUsers());

  @AfterEach
  void afterEach() {
    tlResources
        .get()
        .forEach(
            resource -> {
              try {
                samFixtures.deleteResource(steward(), resource);
              } catch (Exception e) {
                logger.warn("Failed to delete resource: " + resource, e);
              }
            });
  }

  /** verify permissions for CRUD operations on datarepo-google-project */
  @Test
  void verifyGoogleProjectCrud() {
    // create datarepo-google-project, verify permissions
    var project = new SamFixtures.Resource(IamResourceType.GOOGLE_PROJECT);
    addResource(project);
    samFixtures.createResource(steward(), project);

    assertThrows(Exception.class, () -> samFixtures.deleteResource(reader(), project));
  }

  /* create dataset, create child datarepo-google-project, verify permission inheritance */
  @Test
  void verifyDataset() {
    var dataset = new SamFixtures.Resource(IamResourceType.DATASET);
    addResource(dataset);
    samFixtures.createResource(steward(), dataset);

    var project = new SamFixtures.Resource(IamResourceType.GOOGLE_PROJECT);
    addResource(project);
    samFixtures.createResource(steward(), project, dataset);
    assertThrows(Exception.class, () -> samFixtures.deleteResource(reader(), project));

    samFixtures.addUserToResource(reader(), dataset, IamRole.STEWARD);

    assertDoesNotThrow(() -> samFixtures.deleteResource(reader(), project));
    tlResources.get().remove(project);
  }

  // create snapshot, create child datarepo-google-project, verify permission inheritance
  @Test
  void verifySnapshot() {
    var snapshot = new SamFixtures.Resource(IamResourceType.DATASNAPSHOT);
    addResource(snapshot);
    samFixtures.createResource(steward(), snapshot);

    var project = new SamFixtures.Resource(IamResourceType.GOOGLE_PROJECT);
    addResource(project);
    samFixtures.createResource(steward(), project, snapshot);
    assertThrows(Exception.class, () -> samFixtures.deleteResource(reader(), project));

    samFixtures.addUserToResource(reader(), snapshot, IamRole.STEWARD);

    assertDoesNotThrow(() -> samFixtures.deleteResource(reader(), project));
    tlResources.get().remove(project);
  }

  // verify permission inheritance when snapshot is also a child of dataset
  @Test
  void verifyChildSnapshot() {
    var dataset = new SamFixtures.Resource(IamResourceType.DATASET);
    addResource(dataset);
    samFixtures.createResource(steward(), dataset);

    var snapshot = new SamFixtures.Resource(IamResourceType.DATASNAPSHOT);
    addResource(snapshot);
    samFixtures.createResource(reader(), snapshot, dataset);

    var project = new SamFixtures.Resource(IamResourceType.GOOGLE_PROJECT);
    addResource(project);
    samFixtures.createResource(custodian(), project, snapshot);
    assertThrows(Exception.class, () -> samFixtures.deleteResource(reader(), project));

    samFixtures.addUserToResource(reader(), dataset, IamRole.STEWARD);

    assertDoesNotThrow(() -> samFixtures.deleteResource(reader(), project));
    tlResources.get().remove(project);
  }
}
