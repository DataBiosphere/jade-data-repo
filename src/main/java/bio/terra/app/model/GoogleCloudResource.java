package bio.terra.app.model;

/**
 * Google cloud resources used by TDR.
 */
public enum GoogleCloudResource {
  BIGQUERY("bigquery"),
    FIRESTORE("firestore"),
    BUCKET("bucket");

  private String value;

  GoogleCloudResource(String value) {
    this.value = value;
  }

  public String toString() {
    return String.valueOf(value);
  }

  public static GoogleCloudResource fromValue(String text) {
    for (GoogleCloudResource b : GoogleCloudResource.values()) {
      if (String.valueOf(b.value).equals(text)) {
        return b;
      }
    }
    return null;
  }
}
