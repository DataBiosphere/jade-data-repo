package bio.terra.service.filedata.google.gcs;

public class GcsConstants {

  private GcsConstants() {}

  /**
   * Query parameter on gs:// urls that can be used to specify what google project to bill when
   * accessing the blob. Note: this is not a Google-recognized query parameter so it must be removed
   * when accessing the file using Google clients
   */
  public static final String USER_PROJECT_QUERY_PARAM = "userProject";
}
