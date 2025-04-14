package bio.terra.integration;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasItem;

import bio.terra.common.auth.Users;
import bio.terra.common.category.Integration;
import bio.terra.common.configuration.TestConfiguration;
import bio.terra.integration.SamFixtures.Resource;
import bio.terra.service.auth.iam.IamAction;
import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.auth.iam.IamRole;
import java.util.Stack;
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

  private final ThreadLocal<Stack<Resource>> tlResources = ThreadLocal.withInitial(Stack::new);

  private void addResource(Resource resource) {
    tlResources.get().push(resource);
  }

  private TestConfiguration.User owner() {
    return testUsers.get().steward();
  }

  private TestConfiguration.User collaborator() {
    return testUsers.get().reader();
  }

  private final ThreadLocal<Users.TestUsers> testUsers =
      ThreadLocal.withInitial(() -> users.testUsers());

  @AfterEach
  void afterEach() {
    var resources = tlResources.get();
    while (!resources.isEmpty()) {
      var resource = resources.pop();
      try {
        samFixtures.deleteResource(owner(), resource);
      } catch (Exception e) {
        logger.warn("Failed to delete resource: " + resource, e);
      }
    }
  }

  /** verify permissions for CRUD operations on datarepo-google-project */
  @Test
  void verifyGoogleProjectCrud() {
    // create datarepo-google-project, verify permissions
    var project = new Resource(IamResourceType.GOOGLE_PROJECT);
    addResource(project);
    samFixtures.createResource(owner(), project);

    assertThat(
        samFixtures.getResourceActions(owner(), project), hasItem(IamAction.LINK.toString()));
    assertThat(samFixtures.getResourceActions(collaborator(), project), empty());
  }

  /* create dataset, create child datarepo-google-project, verify permission inheritance */
  @Test
  void verifyDataset() {
    var dataset = new Resource(IamResourceType.DATASET);
    addResource(dataset);
    samFixtures.createResource(owner(), dataset);

    var project = new Resource(IamResourceType.GOOGLE_PROJECT);
    addResource(project);
    samFixtures.createResource(owner(), project, dataset);
    var actions = samFixtures.getResourceActions(collaborator(), project);
    assertThat(actions, empty());

    samFixtures.addUserToResource(owner(), dataset, collaborator(), IamRole.STEWARD);

    actions = samFixtures.getResourceActions(collaborator(), dataset);
    assertThat(actions, hasItem(IamAction.INGEST_DATA.toString()));

    actions = samFixtures.getResourceActions(collaborator(), project);
    assertThat(actions, hasItem(IamAction.LINK.toString()));
  }

  // create snapshot, create child datarepo-google-project, verify permission inheritance
  @Test
  void verifySnapshot() {
    var snapshot = new Resource(IamResourceType.DATASNAPSHOT);
    addResource(snapshot);
    samFixtures.createResource(owner(), snapshot);

    var project = new Resource(IamResourceType.GOOGLE_PROJECT);
    addResource(project);
    samFixtures.createResource(owner(), project, snapshot);
    var actions = samFixtures.getResourceActions(collaborator(), project);
    assertThat(actions, empty());

    samFixtures.addUserToResource(owner(), snapshot, collaborator(), IamRole.STEWARD);

    actions = samFixtures.getResourceActions(collaborator(), snapshot);
    assertThat(actions, hasItem(IamAction.DELETE.toString()));

    actions = samFixtures.getResourceActions(collaborator(), project);
    assertThat(actions, hasItem(IamAction.LINK.toString()));
  }

  // verify permission inheritance when snapshot is also a child of dataset
  @Test
  void verifyChildSnapshot() {
    var dataset = new Resource(IamResourceType.DATASET);
    addResource(dataset);
    samFixtures.createResource(owner(), dataset);

    var snapshot = new Resource(IamResourceType.DATASNAPSHOT);
    addResource(snapshot);
    samFixtures.createResource(owner(), snapshot, dataset);

    var project = new Resource(IamResourceType.GOOGLE_PROJECT);
    addResource(project);
    var actions = samFixtures.getResourceActions(collaborator(), project);
    assertThat(actions, empty());

    samFixtures.addUserToResource(owner(), dataset, collaborator(), IamRole.STEWARD);

    actions = samFixtures.getResourceActions(collaborator(), dataset);
    assertThat(actions, hasItem(IamAction.INGEST_DATA.toString()));

    actions = samFixtures.getResourceActions(collaborator(), snapshot);
    assertThat(actions, hasItem(IamAction.VIEW_JOURNAL.toString()));

    actions = samFixtures.getResourceActions(collaborator(), project);
    assertThat(actions, hasItem(IamAction.LINK.toString()));
  }
}
