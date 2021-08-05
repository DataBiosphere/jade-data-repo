package bio.terra.app.controller;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import bio.terra.common.category.Unit;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;

@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@Category(Unit.class)
public class AssetModelValidationTest {

  @Autowired private MockMvc mvc;

  @Test
  public void testInvalidAssetRequest() throws Exception {
    mvc.perform(
            post("/api/repository/v1/datasets/assets")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{}"))
        .andExpect(status().is4xxClientError());
  }
}
