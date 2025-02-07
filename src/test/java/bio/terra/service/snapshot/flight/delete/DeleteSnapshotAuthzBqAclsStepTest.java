package bio.terra.service.snapshot.flight.delete;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.common.fixtures.AuthenticationFixtures;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.auth.iam.IamRole;
import bio.terra.service.auth.iam.IamService;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.service.resourcemanagement.google.GoogleProjectResource;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.service.snapshot.SnapshotSource;
import bio.terra.stairway.StepResult;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@Tag(Unit.TAG)
class DeleteSnapshotAuthzBqAclsStepTest {
  @Mock private IamService sam;
  @Mock private ResourceService resourceService;
  @Mock private SnapshotService snapshotService;

  private static final AuthenticatedUserRequest TEST_USER =
      AuthenticationFixtures.randomUserRequest();
  private static final Dataset DATASET = new Dataset().id(UUID.randomUUID());
  private static final String GOOGLE_PROJECT_ID = "googleProjectId";
  private static final Snapshot SNAPSHOT =
      new Snapshot()
          .id(UUID.randomUUID())
          .snapshotSources(List.of(new SnapshotSource().dataset(DATASET)))
          .projectResource(new GoogleProjectResource().googleProjectId(GOOGLE_PROJECT_ID));

  private DeleteSnapshotAuthzBqAclsStep step;

  @BeforeEach
  void beforeEach() {
    step =
        new DeleteSnapshotAuthzBqAclsStep(
            sam, resourceService, snapshotService, SNAPSHOT.getId(), TEST_USER);
  }

  @Test
  void doStep() throws Exception {
    when(snapshotService.retrieve(SNAPSHOT.getId())).thenReturn(SNAPSHOT);
    when(sam.retrievePolicyEmails(TEST_USER, IamResourceType.DATASNAPSHOT, SNAPSHOT.getId()))
        .thenReturn(Map.of(IamRole.STEWARD, "steward", IamRole.READER, "reader"));
    when(sam.retrievePolicyEmails(TEST_USER, IamResourceType.DATASET, DATASET.getId()))
        .thenReturn(Map.of(IamRole.CUSTODIAN, "custodian"));
    assertThat(step.doStep(null), is(StepResult.getStepResultSuccess()));
    verify(resourceService)
        .revokePoliciesBqJobUser(GOOGLE_PROJECT_ID, List.of("steward", "reader", "custodian"));
  }

  @Test
  void undoStep() {
    assertThat(step.undoStep(null), is(StepResult.getStepResultSuccess()));
  }
}
