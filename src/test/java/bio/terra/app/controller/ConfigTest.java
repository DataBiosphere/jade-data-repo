package bio.terra.app.controller;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import bio.terra.common.category.Unit;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;

@RunWith(SpringRunner.class)
@SpringBootTest(
    properties = {"oauth.clientId=whateverstringyoulike", "datarepo.testWithDatabase=false"})
@AutoConfigureMockMvc
@ActiveProfiles({"google", "unittest"})
@Category(Unit.class)
public class ConfigTest {

  @Autowired private MockMvc mvc;

  @Test
  public void testConfigOK() throws Exception {
    this.mvc.perform(get("/configuration")).andExpect(status().isOk());
  }

  @Test
  public void testConfigReturnsModel() throws Exception {
    this.mvc
        .perform(get("/configuration"))
        .andExpect(jsonPath("$.clientId").value("whateverstringyoulike"));
  }
}
