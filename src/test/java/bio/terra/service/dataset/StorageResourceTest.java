package bio.terra.service.dataset;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;

import bio.terra.app.model.AzureCloudResource;
import bio.terra.app.model.AzureRegion;
import bio.terra.app.model.GoogleCloudResource;
import bio.terra.app.model.GoogleRegion;
import bio.terra.common.category.Unit;
import bio.terra.common.fixtures.JsonLoader;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.List;
import java.util.UUID;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest(properties = {"datarepo.testWithEmbeddedDatabase=false"})
@AutoConfigureMockMvc
@ActiveProfiles({"google", "unittest"})
@Category(Unit.class)
public class StorageResourceTest {

  @Autowired private JsonLoader jsonLoader;

  @Autowired private ObjectMapper objectMapper;

  private final List<? extends StorageResource<?, ?>> model =
      List.of(
          new AzureStorageResource(
              UUID.fromString("a3d54871-8cdc-4549-8410-28005df9cbaf"),
              AzureCloudResource.APPLICATION_DEPLOYMENT,
              AzureRegion.CENTRAL_US),
          new GoogleStorageResource(
              UUID.fromString("a3d54871-8cdc-4549-8410-28005df9cbaf"),
              GoogleCloudResource.BUCKET,
              GoogleRegion.US_EAST1));

  @Test
  public void testDeserialization() throws IOException {
    List<? extends StorageResource<?, ?>> storageResource =
        jsonLoader.loadObject("storage-account.json", new TypeReference<>() {});
    assertThat(storageResource, equalTo(model));
  }

  @Test
  public void testDeserializationMixedCloudResourcesFail() throws IOException {
    assertThrows(
        JsonMappingException.class,
        () ->
            jsonLoader.loadObject(
                "storage-account.mixedcloud.json", new TypeReference<List<StorageResource>>() {}),
        "mixed cloud resources don't deserialize");
  }

  @Test
  public void testSerialization() throws IOException {
    String json = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(model);
    System.err.println(json);
    List<StorageResource> storageResource = objectMapper.readValue(json, new TypeReference<>() {});
    assertThat(storageResource, equalTo(model));
  }
}
