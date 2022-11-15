package bio.terra.service.duos;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.startsWith;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.model.DuosFirecloudGroupModel;
import bio.terra.model.RepositoryStatusModelSystems;
import bio.terra.service.auth.iam.IamService;
import bio.terra.service.auth.iam.exception.IamConflictException;
import bio.terra.service.auth.iam.exception.IamForbiddenException;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.http.HttpStatus;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.web.client.HttpServerErrorException;

@RunWith(MockitoJUnitRunner.StrictStubs.class)
@ActiveProfiles({"google", "unittest"})
@Category(Unit.class)
public class DuosServiceTest {

  @Mock private IamService iamService;
  @Mock private DuosClient duosClient;
  private DuosService duosService;

  private static final List<String> SUBSYSTEM_NAMES =
      List.of("ontology", "elastic-search", "google-cloud-storage");
  private static final String DUOS_ID = "DUOS-123456";
  private static final String FIRECLOUD_GROUP_NAME = String.format("%s-users", DUOS_ID);
  private static final String FIRECLOUD_GROUP_EMAIL = firecloudGroupEmail(FIRECLOUD_GROUP_NAME);

  @Before
  public void before() {
    duosService = new DuosService(iamService, duosClient);
  }

  private static SystemStatusSystems statusSubsystem(boolean healthy, String name) {
    String message = name + " " + (healthy ? "" : "un") + "healthy";
    return new SystemStatusSystems(healthy, message, null, null);
  }

  @Test
  public void testStatusOk() {
    Map<String, SystemStatusSystems> subsystems =
        SUBSYSTEM_NAMES.stream()
            .collect(Collectors.toMap(Function.identity(), name -> statusSubsystem(true, name)));
    SystemStatus systemStatus = new SystemStatus(true, false, subsystems);
    when(duosClient.status()).thenReturn(systemStatus);

    RepositoryStatusModelSystems actual = duosService.status();
    assertTrue(actual.isOk());
    assertThat(actual.isCritical(), equalTo(DuosService.IS_CRITICAL_SYSTEM));
    assertThat(actual.getMessage(), equalTo(subsystems.toString()));
  }

  @Test
  public void testStatusNotOk() {
    Map<String, SystemStatusSystems> subsystems =
        SUBSYSTEM_NAMES.stream()
            .collect(Collectors.toMap(Function.identity(), name -> statusSubsystem(false, name)));
    SystemStatus systemStatus = new SystemStatus(false, false, subsystems);
    when(duosClient.status()).thenReturn(systemStatus);

    RepositoryStatusModelSystems actual = duosService.status();
    assertFalse(actual.isOk());
    assertThat(actual.isCritical(), equalTo(DuosService.IS_CRITICAL_SYSTEM));
    assertThat(actual.getMessage(), equalTo(subsystems.toString()));
  }

  @Test
  public void testStatusNotOkWhenDuosClientThrows() {
    String exceptionMessage = "Error thrown by DUOS";
    when(duosClient.status())
        .thenThrow(
            new HttpServerErrorException(HttpStatus.INTERNAL_SERVER_ERROR, exceptionMessage));

    RepositoryStatusModelSystems actual = duosService.status();
    assertFalse(actual.isOk());
    assertThat(actual.isCritical(), equalTo(DuosService.IS_CRITICAL_SYSTEM));
    assertThat(actual.getMessage(), containsString(exceptionMessage));
  }

  private static String firecloudGroupEmail(String groupName) {
    return String.format("%s@dev.test.firecloud.org", groupName);
  }

  @Test
  public void testCreateFirecloudGroup() {
    when(iamService.createGroup(FIRECLOUD_GROUP_NAME)).thenReturn(FIRECLOUD_GROUP_EMAIL);

    DuosFirecloudGroupModel actual = duosService.createFirecloudGroup(DUOS_ID);
    assertThat(actual.getDuosId(), equalTo(DUOS_ID));
    assertThat(actual.getFirecloudGroupName(), equalTo(FIRECLOUD_GROUP_NAME));
    assertThat(actual.getFirecloudGroupEmail(), equalTo(FIRECLOUD_GROUP_EMAIL));
    verify(iamService).createGroup(startsWith(FIRECLOUD_GROUP_NAME));
  }

  @Test
  public void testCreateFirecloudGroupWithNamingConflict() {
    String groupEmailNew = firecloudGroupEmail(FIRECLOUD_GROUP_NAME + "-new");

    // If we encounter a naming collision when trying to create a Firecloud group,
    // we don't know exactly what our backstop name will be as it will be suffixed by a new UUID,
    // but we know that it will start with our more readable group name.
    when(iamService.createGroup(startsWith(FIRECLOUD_GROUP_NAME))).thenReturn(groupEmailNew);
    IamConflictException iamConflictEx =
        new IamConflictException("Group already existed", List.of());
    doThrow(iamConflictEx).when(iamService).createGroup(FIRECLOUD_GROUP_NAME);

    DuosFirecloudGroupModel actual = duosService.createFirecloudGroup(DUOS_ID);
    assertThat(actual.getDuosId(), equalTo(DUOS_ID));
    assertThat(actual.getFirecloudGroupName(), Matchers.startsWith(FIRECLOUD_GROUP_NAME));
    assertThat(actual.getFirecloudGroupEmail(), equalTo(groupEmailNew));
    // Our first creation attempt failed, so we tried to create again with our unique group name.
    verify(iamService, times(2)).createGroup(startsWith(FIRECLOUD_GROUP_NAME));
  }

  @Test
  public void testCreateFirecloudGroupWithUnretriableCreationException() {
    IamForbiddenException iamForbiddenException =
        new IamForbiddenException("Unexpected SAM error", List.of());
    doThrow(iamForbiddenException).when(iamService).createGroup(FIRECLOUD_GROUP_NAME);
    assertThrows(IamForbiddenException.class, () -> duosService.createFirecloudGroup(DUOS_ID));
    verify(iamService).createGroup(startsWith(FIRECLOUD_GROUP_NAME));
  }
}
