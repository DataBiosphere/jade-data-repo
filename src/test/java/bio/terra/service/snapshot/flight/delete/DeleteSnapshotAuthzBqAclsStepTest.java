package bio.terra.service.snapshot.flight.delete;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.reset;
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
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
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
    when(snapshotService.retrieve(SNAPSHOT.getId())).thenReturn(SNAPSHOT);
    when(sam.retrievePolicyEmails(TEST_USER, IamResourceType.DATASNAPSHOT, SNAPSHOT.getId()))
        .thenReturn(Map.of(IamRole.STEWARD, "steward", IamRole.READER, "reader"));
    step =
        new DeleteSnapshotAuthzBqAclsStep(
            sam, resourceService, snapshotService, SNAPSHOT.getId(), TEST_USER);
  }

  private static Stream<Arguments> doStep() {
    return Stream.of(
        Arguments.of(
            Map.of(IamRole.CUSTODIAN, "custodian"), List.of("steward", "reader", "custodian")),
        Arguments.of(Map.of(), List.of("steward", "reader")));
  }

  @MethodSource
  @ParameterizedTest
  void doStep(Map<IamRole, String> custodianPolicy, List<String> expectedEmails) throws Exception {
    when(sam.retrievePolicyEmails(TEST_USER, IamResourceType.DATASET, DATASET.getId()))
        .thenReturn(custodianPolicy);
    assertThat(step.doStep(null), is(StepResult.getStepResultSuccess()));
    ArgumentCaptor<List<String>> argument = ArgumentCaptor.captor();
    verify(resourceService).revokePoliciesBqJobUser(eq(GOOGLE_PROJECT_ID), argument.capture());
    assertThat(argument.getValue(), containsInAnyOrder(expectedEmails.toArray()));
  }

  @Test
  void undoStep() {
    reset(snapshotService, sam);
    assertThat(step.undoStep(null), is(StepResult.getStepResultSuccess()));
  }
}
