package bio.terra.service.dataset;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import bio.terra.app.model.AzureCloudResource;
import bio.terra.app.model.AzureRegion;
import bio.terra.app.model.GoogleCloudResource;
import bio.terra.app.model.GoogleRegion;
import bio.terra.common.TestUtils;
import bio.terra.common.category.Unit;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@Tag(Unit.TAG)
class StorageResourceTest {

  private static final List<? extends StorageResource<?, ?>> MODEL =
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
  void testDeserialization() {
    List<? extends StorageResource<?, ?>> storageResource =
        TestUtils.loadObject("storage-account.json", new TypeReference<>() {});
    assertThat(storageResource, equalTo(MODEL));
  }

  @Test
  void testDeserializationMixedCloudResourcesFail() {
    TypeReference<List<StorageResource<?, ?>>> typeReference = new TypeReference<>() {};
    RuntimeException ex =
        assertThrows(
            RuntimeException.class,
            () -> TestUtils.loadObject("storage-account.mixedcloud.json", typeReference),
            "mixed cloud resources don't deserialize");
    assertThat(ex.getCause(), instanceOf(JsonMappingException.class));
  }

  @Test
  void testSerialization() throws JsonProcessingException {
    ObjectMapper objectMapper = new ObjectMapper();
    String json = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(MODEL);
    List<StorageResource<?, ?>> storageResource =
        objectMapper.readValue(json, new TypeReference<>() {});
    assertThat(storageResource, equalTo(MODEL));
  }
}
