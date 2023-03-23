package bio.terra.common.configuration;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@EnableConfigurationProperties
@ConfigurationProperties(prefix = "it")
public class TestConfiguration {
  private static Logger logger = LoggerFactory.getLogger(TestConfiguration.class);
  private static final Pattern INT_SERVER_NUM_FINDER =
      Pattern.compile("https://jade-(\\d+).datarepo-integration.broadinstitute.org");

  private String jadeApiUrl;
  private String jadePemFileName;
  private String jadeEmail;
  private String ingestbucket;
  private List<User> users = new ArrayList<>();
  private String googleProjectId;
  private String googleBillingAccountId;
  private UUID targetTenantId;
  private UUID targetSubscriptionId;
  private String targetResourceGroupName;
  private String targetManagedResourceGroupName;
  private String targetApplicationName;
  private String sourceStorageAccountName;
  private String ingestRequestContainer;

  public static class User {
    private String role;
    private String name;
    private String email;
    private String subjectId;

    public String getRole() {
      return role;
    }

    public void setRole(String role) {
      this.role = role;
    }

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public String getEmail() {
      return email;
    }

    public void setEmail(String email) {
      this.email = email;
    }

    public String getSubjectId() {
      return subjectId;
    }

    public void setSubjectId(String subjectId) {
      this.subjectId = subjectId;
    }
  }

  public String getJadeApiUrl() {
    return jadeApiUrl;
  }

  public void setJadeApiUrl(String jadeApiUrl) {
    this.jadeApiUrl = jadeApiUrl;
  }

  public String getJadePemFileName() {
    return jadePemFileName;
  }

  public void setJadePemFileName(String jadePemFileName) {
    this.jadePemFileName = jadePemFileName;
  }

  public String getJadeEmail() {
    return jadeEmail;
  }

  public void setJadeEmail(String jadeSAEmail) {
    this.jadeEmail = jadeSAEmail;
  }

  public String getIngestbucket() {
    return ingestbucket;
  }

  public void setIngestbucket(String ingestbucket) {
    this.ingestbucket = ingestbucket;
  }

  public List<User> getUsers() {
    return users;
  }

  public void setUsers(List<User> users) {
    this.users = users;
  }

  public String getGoogleProjectId() {
    return googleProjectId;
  }

  public void setGoogleProjectId(String googleProjectId) {
    this.googleProjectId = googleProjectId;
  }

  public String getGoogleBillingAccountId() {
    return googleBillingAccountId;
  }

  public void setGoogleBillingAccountId(String googleBillingAccountId) {
    this.googleBillingAccountId = googleBillingAccountId;
  }

  public UUID getTargetTenantId() {
    return targetTenantId;
  }

  public void setTargetTenantId(UUID targetTenantId) {
    this.targetTenantId = targetTenantId;
  }

  public UUID getTargetSubscriptionId() {
    return targetSubscriptionId;
  }

  public void setTargetSubscriptionId(UUID targetSubscriptionId) {
    this.targetSubscriptionId = targetSubscriptionId;
  }

  public String getTargetResourceGroupName() {
    return targetResourceGroupName;
  }

  public void setTargetResourceGroupName(String targetResourceGroupName) {
    this.targetResourceGroupName = targetResourceGroupName;
  }

  public String getTargetManagedResourceGroupName() {
    return targetManagedResourceGroupName;
  }

  public void setTargetManagedResourceGroupName(String targetManagedResourceGroupName) {
    this.targetManagedResourceGroupName = targetManagedResourceGroupName;
  }

  public String getTargetApplicationName() {
    return targetApplicationName;
  }

  public void setTargetApplicationName(String targetApplicationName) {
    this.targetApplicationName = targetApplicationName;
  }

  public String getSourceStorageAccountName() {
    return sourceStorageAccountName;
  }

  public void setSourceStorageAccountName(String sourceStorageAccountName) {
    this.sourceStorageAccountName = sourceStorageAccountName;
  }

  public String getIngestRequestContainer() {
    return ingestRequestContainer;
  }

  public void setIngestRequestContainer(String ingestRequestContainer) {
    this.ingestRequestContainer = ingestRequestContainer;
  }

  /**
   * Returns the server number that the test is running on or null if the URL isn't in the expected
   * format
   */
  public Integer getIntegrationServerNumber() {
    Matcher matcher = INT_SERVER_NUM_FINDER.matcher(getJadeApiUrl());
    if (matcher.find()) {
      return Integer.getInteger(matcher.group());
    }
    logger.warn("Can't get integration server number from url {}", getJadeApiUrl());
    return null;
  }
}
