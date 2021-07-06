package bio.terra.app.model;

/** Google cloud resources used by TDR. */
public enum GoogleCloudResource implements CloudResource {
  BIGQUERY("bigquery"),
  FIRESTORE("firestore"),
  BUCKET("bucket");

  private final String value;

  GoogleCloudResource(String value) {
    this.value = value;
  }

  public String toString() {
    return value;
  }

  @Override
  public String getValue() {
    return value;
  }

  public static GoogleCloudResource fromValue(String text) {
    for (GoogleCloudResource resource : GoogleCloudResource.values()) {
      if (resource.value.equalsIgnoreCase(text)) {
        return resource;
      }
    }
    return null;
  }
}
