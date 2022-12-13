package bio.terra.app.controller;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.startsWith;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import bio.terra.common.category.Unit;
import bio.terra.model.RepositoryStatusModelSystems;
import bio.terra.service.auth.iam.IamProviderInterface;
import bio.terra.service.configuration.ConfigEnum;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.dataset.DatasetDao;
import bio.terra.service.duos.DuosService;
import bio.terra.service.resourcemanagement.BufferService;
import bio.terra.service.status.StatusService;
import org.hamcrest.Matcher;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

@RunWith(SpringRunner.class)
@SpringBootTest(properties = {"datarepo.testWithEmbeddedDatabase=false"})
@AutoConfigureMockMvc
@ActiveProfiles({"google", "unittest"})
@Category(Unit.class)
public class StatusTest {

  @Autowired private MockMvc mvc;
  @Autowired private ConfigurationService configurationService;
  @MockBean private DatasetDao datasetDao;
  @MockBean private IamProviderInterface iamProviderInterface;
  @MockBean private BufferService bufferService;
  @MockBean private DuosService duosService;

  private static RepositoryStatusModelSystems ok() {
    return new RepositoryStatusModelSystems().ok(true);
  }

  @Before
  public void setup() throws Exception {
    when(datasetDao.statusCheck()).thenReturn(ok());
    when(iamProviderInterface.samStatus()).thenReturn(ok());
    when(bufferService.status()).thenReturn(ok().critical(false));
    when(duosService.status()).thenReturn(ok().critical(false));
  }

  @Test
  public void testStatus() throws Exception {
    this.mvc.perform(get("/status")).andExpect(status().isOk());
  }

  @Test
  public void testStatusDown() throws Exception {
    configurationService.setFault(ConfigEnum.LIVENESS_FAULT.name(), true);
    this.mvc.perform(get("/status")).andExpect(status().is5xxServerError());
    configurationService.setFault(ConfigEnum.LIVENESS_FAULT.name(), false);
    this.mvc.perform(get("/status")).andExpect(status().isOk());
  }

  @Test
  public void testCriticalSystemDown() throws Exception {
    when(datasetDao.statusCheck())
        .thenReturn(new RepositoryStatusModelSystems().ok(false).critical(true));
    MvcResult result =
        this.mvc.perform(get("/status")).andExpect(status().is5xxServerError()).andReturn();
    MockHttpServletResponse response = result.getResponse();
    String responseBody = response.getContentAsString();
    assertThat(
        "/Status response should indicate that the whole system is down.",
        responseBody,
        startsWith("{\"ok\":false"));
    assertThat(
        "/Status response should indicate that postgres is down",
        responseBody,
        containsSubserviceString(StatusService.POSTGRES, false, true));
    assertThat(
        "/Status response should indicate that sam is up",
        responseBody,
        containsSubserviceString(StatusService.SAM, true, true));
    assertThat(
        "/Status response should indicate that rbs is up",
        responseBody,
        containsSubserviceString(StatusService.RBS, true, false));
    assertThat(
        "/Status response should indicate that duos is up",
        responseBody,
        containsSubserviceString(StatusService.DUOS, true, false));
  }

  @Test
  public void testNonCriticalSystemDown() throws Exception {
    when(duosService.status())
        .thenReturn(new RepositoryStatusModelSystems().ok(false).critical(false));
    MvcResult result = this.mvc.perform(get("/status")).andExpect(status().isOk()).andReturn();
    MockHttpServletResponse response = result.getResponse();
    String responseBody = response.getContentAsString();
    assertThat(
        "/Status response should indicate that the system is up.",
        responseBody,
        startsWith("{\"ok\":true"));
    assertThat(
        "/Status response should indicate that postgres is up",
        responseBody,
        containsSubserviceString(StatusService.POSTGRES, true, true));
    assertThat(
        "/Status response should indicate that sam is up",
        responseBody,
        containsSubserviceString(StatusService.SAM, true, true));
    assertThat(
        "/Status response should indicate that rbs is up",
        responseBody,
        containsSubserviceString(StatusService.RBS, true, false));
    assertThat(
        "/Status response should indicate that duos is down",
        responseBody,
        containsSubserviceString(StatusService.DUOS, false, false));
  }

  private Matcher<String> containsSubserviceString(
      String subserviceName, boolean ok, boolean critical) {
    String expected = "\"%s\":{\"ok\":%b,\"critical\":%b".formatted(subserviceName, ok, critical);
    return containsString(expected);
  }
}
