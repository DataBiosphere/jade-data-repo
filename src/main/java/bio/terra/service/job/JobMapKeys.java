package bio.terra.service.job;

import bio.terra.model.BillingProfileModel;
import bio.terra.model.CloudPlatform;
import bio.terra.service.iam.AuthenticatedUserRequest;
import bio.terra.stairway.FlightFilter;
import bio.terra.stairway.FlightFilterOp;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.exception.FlightFilterException;
import com.fasterxml.jackson.core.type.TypeReference;
import java.util.UUID;
import org.springframework.http.HttpStatus;

public enum JobMapKeys {
  // parameters for all flight types
  DESCRIPTION("", new TypeReference<>() {}),
  REQUEST,
  REVERT_TO(new BillingProfileModel(), new TypeReference<>() {}),
  RESPONSE,
  STATUS_CODE(HttpStatus.OK, new TypeReference<>() {}),
  AUTH_USER_INFO(new AuthenticatedUserRequest(), new TypeReference<>() {}),
  SUBJECT_ID("", new TypeReference<>() {}),
  CLOUD_PLATFORM(CloudPlatform.GCP, new TypeReference<>() {}),

  // parameters for specific flight types
  DATASET_ID(new UUID(0, 0), new TypeReference<>() {}),
  SNAPSHOT_ID(new UUID(0, 0), new TypeReference<>() {}),
  FILE_ID("", new TypeReference<>() {}),
  ASSET_ID(new UUID(0, 0), new TypeReference<>() {});

  private final String keyName;
  private final TypeReference<?> typeReference;

  JobMapKeys() {
    this(null, new TypeReference<>() {});
  }

  <T> JobMapKeys(T t, TypeReference<T> typeReference) {
    this.keyName = name().toLowerCase();
    this.typeReference = typeReference;
  }

  private void checkType(Object value) {
    if (!((Class<?>) typeReference.getType()).isAssignableFrom(value.getClass())) {
      throw new RuntimeException("invalid typed object " + value);
    }
  }

  public void put(FlightMap map, Object value) {
    checkType(value);
    map.put(keyName, value);
  }

  public void addFilter(FlightFilter filter, FlightFilterOp filterOp, Object value)
      throws FlightFilterException {
    checkType(value);
    filter.addFilterInputParameter(keyName, filterOp, value);
  }

  public <T> T get(FlightMap map) {
    //noinspection unchecked
    return map.get(keyName, (TypeReference<? extends T>) typeReference);
  }
}
