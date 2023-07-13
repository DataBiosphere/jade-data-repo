package bio.terra.service.filedata.azure.util;

public class AzureConstants {

  private AzureConstants() {}

  /** The code that the Azure resource management APIs found when a resource is not found */
  public static final String RESOURCE_NOT_FOUND_CODE = "ResourceNotFound";
  /**
   * The code that the Azure security insights management APIs found when a resource is not found
   */
  public static final String NOT_FOUND_CODE = "NotFound";
  /**
   * The code that the Azure resource management APIs found when an invalid request is sent. This
   * can sometimes be equivalent to a 404
   */
  public static final String BAD_REQUEST_CODE = "BadRequest";
}
