package bio.terra.service.status;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.model.RepositoryStatusModelSystems;
import bio.terra.service.auth.iam.IamProviderInterface;
import bio.terra.service.dataset.DatasetDao;
import bio.terra.service.duos.DuosService;
import bio.terra.service.policy.PolicyService;
import bio.terra.service.resourcemanagement.BufferService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@Tag(Unit.TAG)
class StatusServiceTest {

  @Mock private DatasetDao datasetDao;
  @Mock private IamProviderInterface iamProviderInterface;
  @Mock private BufferService bufferService;
  @Mock private DuosService duosService;
  @Mock private PolicyService policyService;

  private StatusService statusService;

  @BeforeEach
  void setup() {
    statusService =
        new StatusService(
            datasetDao, iamProviderInterface, bufferService, duosService, policyService);

    when(datasetDao.statusCheck()).thenReturn(ok());
    when(iamProviderInterface.samStatus()).thenReturn(ok());
    when(bufferService.status()).thenReturn(ok().critical(false));
    when(duosService.status()).thenReturn(ok().critical(false));
    when(policyService.status()).thenReturn(ok().critical(true));
  }

  private static RepositoryStatusModelSystems ok() {
    return new RepositoryStatusModelSystems().ok(true);
  }

  private static RepositoryStatusModelSystems notOk() {
    return new RepositoryStatusModelSystems().ok(false);
  }

  private void verifySystemStatusCalls() {
    verify(datasetDao).statusCheck();
    verify(iamProviderInterface).samStatus();
    verify(bufferService).status();
    verify(duosService).status();
    verify(policyService).status();
  }

  @Test
  void testStatusAllSystemsUp() {
    var status = statusService.getStatus();
    assertThat("TDR is up when all systems are up", status.isOk(), is(true));
    var systems = status.getSystems();
    assertThat(
        "All expected systems are included in status check",
        systems.keySet(),
        containsInAnyOrder(
            StatusService.POSTGRES,
            StatusService.SAM,
            StatusService.RBS,
            StatusService.DUOS,
            StatusService.TPS));

    assertThat(
        "Postgres is up and critical by default",
        systems.get(StatusService.POSTGRES),
        equalTo(ok().critical(true)));
    assertThat(
        "Sam is up and critical by default",
        systems.get(StatusService.SAM),
        equalTo(ok().critical(true)));
    assertThat(
        "RBS is up and not critical by default",
        systems.get(StatusService.RBS),
        equalTo(ok().critical(false)));
    assertThat(
        "DUOS is up and not critical by default",
        systems.get(StatusService.DUOS),
        equalTo(ok().critical(false)));
    assertThat(
        "TPS is up and is critical by default",
        systems.get(StatusService.TPS),
        equalTo(ok().critical(true)));

    verifySystemStatusCalls();
  }

  @Test
  void testStatusCriticalSystemDown() {
    when(datasetDao.statusCheck()).thenReturn(notOk());

    var status = statusService.getStatus();
    assertThat("TDR is down when one critical system is down", status.isOk(), is(false));
    var systems = status.getSystems();
    assertThat(
        "All expected systems are included in status check",
        systems.keySet(),
        containsInAnyOrder(
            StatusService.POSTGRES,
            StatusService.SAM,
            StatusService.RBS,
            StatusService.DUOS,
            StatusService.TPS));

    assertThat(
        "Postgres is down and critical by default",
        systems.get(StatusService.POSTGRES),
        equalTo(notOk().critical(true)));
    assertThat(
        "Sam is up and critical by default",
        systems.get(StatusService.SAM),
        equalTo(ok().critical(true)));
    assertThat(
        "RBS is up and not critical by default",
        systems.get(StatusService.RBS),
        equalTo(ok().critical(false)));
    assertThat(
        "DUOS is up and not critical by default",
        systems.get(StatusService.DUOS),
        equalTo(ok().critical(false)));
    assertThat(
        "TPS is up and is critical by default",
        systems.get(StatusService.TPS),
        equalTo(ok().critical(true)));

    verifySystemStatusCalls();
  }

  @Test
  void testStatusNonCriticalSystemDown() {
    when(duosService.status()).thenReturn(notOk().critical(false));

    var status = statusService.getStatus();
    assertThat("TDR is up even if non-critical systems are down", status.isOk(), is(true));
    var systems = status.getSystems();
    assertThat(
        "All expected systems are included in status check",
        systems.keySet(),
        containsInAnyOrder(
            StatusService.POSTGRES,
            StatusService.SAM,
            StatusService.RBS,
            StatusService.DUOS,
            StatusService.TPS));

    assertThat(
        "Postgres is up and critical by default",
        systems.get(StatusService.POSTGRES),
        equalTo(ok().critical(true)));
    assertThat(
        "Sam is up and critical by default",
        systems.get(StatusService.SAM),
        equalTo(ok().critical(true)));
    assertThat(
        "RBS is up and not critical by default",
        systems.get(StatusService.RBS),
        equalTo(ok().critical(false)));
    assertThat(
        "DUOS is down and not critical by default",
        systems.get(StatusService.DUOS),
        equalTo(notOk().critical(false)));
    assertThat(
        "TPS is up and is critical by default",
        systems.get(StatusService.TPS),
        equalTo(ok().critical(true)));

    verifySystemStatusCalls();
  }
}
