package bio.terra.app.controller;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.startsWith;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import bio.terra.common.category.Unit;
import bio.terra.model.RepositoryStatusModelSystems;
import bio.terra.service.configuration.ConfigEnum;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.dataset.DatasetDao;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
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
  @Autowired private DatasetDao datasetDao;

  @Before
  public void setup() throws Exception {
    when(datasetDao.statusCheck()).thenReturn(new RepositoryStatusModelSystems().ok(true));
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
  public void testCriticalSystem() throws Exception {
    configurationService.setFault(ConfigEnum.CRITICAL_SYSTEM_FAULT.name(), true);
    MvcResult result =
        this.mvc.perform(get("/status")).andExpect(status().is5xxServerError()).andReturn();
    MockHttpServletResponse downResponse = result.getResponse();
    String responseBody = downResponse.getContentAsString();
    assertThat(
        "/Status response should indicate that the whole system is down.",
        responseBody,
        startsWith("{\"ok\":false"));
    assertThat(
        "/Status response should indicate that postgres is down",
        responseBody,
        containsString("\"Postgres\":{\"ok\":false,\"critical\":true"));
    assertThat(
        "/Status response should indicate that sam is up",
        responseBody,
        containsString("\"Sam\":{\"ok\":true,\"critical\":true"));
    assertThat(
        "/Status response should indicate that rbs is up",
        responseBody,
        containsString("\"ResourceBufferService\":{\"ok\":true,\"critical\":false"));

    configurationService.setFault(ConfigEnum.CRITICAL_SYSTEM_FAULT.name(), false);
    MvcResult upResult = this.mvc.perform(get("/status")).andExpect(status().isOk()).andReturn();
    MockHttpServletResponse upResponse = upResult.getResponse();
    String upResponseBody = upResponse.getContentAsString();
    assertThat(
        "/Status response should indicate that the whole system is up",
        upResponseBody,
        startsWith("{\"ok\":true"));
  }
}
