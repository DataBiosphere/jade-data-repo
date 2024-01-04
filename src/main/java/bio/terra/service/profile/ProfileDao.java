package bio.terra.service.profile;

import bio.terra.common.DaoKeyHolder;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.BillingProfileRequestModel;
import bio.terra.model.BillingProfileUpdateModel;
import bio.terra.model.CloudPlatform;
import bio.terra.model.EnumerateBillingProfileModel;
import bio.terra.service.profile.exception.ProfileInUseException;
import bio.terra.service.profile.exception.ProfileNotFoundException;
import bio.terra.service.snapshot.exception.CorruptMetadataException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Repository
public class ProfileDao {
  private final NamedParameterJdbcTemplate jdbcTemplate;

  // SQL select string constants
  private static final String SQL_SELECT_LIST =
      "id, name, biller, billing_account_id, description, cloud_platform, "
          + "tenant_id, subscription_id, resource_group_name, application_deployment_name, created_date, created_by";

  private static final String SQL_GET =
      "SELECT " + SQL_SELECT_LIST + " FROM billing_profile WHERE id = :id";

  private static final String SQL_LIST =
      "SELECT "
          + SQL_SELECT_LIST
          + " FROM billing_profile"
          + " WHERE id in (:idlist)"
          + " OFFSET :offset LIMIT :limit";

  private static final String SQL_TOTAL =
      "SELECT count(id) AS total" + " FROM billing_profile" + " WHERE id in (:idlist)";

  private static final String SQL_LIST_ALL = "SELECT " + SQL_SELECT_LIST + " FROM billing_profile";

  @Autowired
  public ProfileDao(NamedParameterJdbcTemplate jdbcTemplate) {
    this.jdbcTemplate = jdbcTemplate;
  }

  @Transactional(propagation = Propagation.REQUIRED, isolation = Isolation.SERIALIZABLE)
  public BillingProfileModel createBillingProfile(
      BillingProfileRequestModel profileRequest, String creator) {
    String sql =
        "INSERT INTO billing_profile"
            + " (id, name, biller, billing_account_id, description, cloud_platform, "
            + "     tenant_id, subscription_id, resource_group_name, application_deployment_name, created_by) VALUES "
            + " (:id, :name, :biller, :billing_account_id, :description, :cloud_platform, "
            + "     :tenant_id, :subscription_id, :resource_group_name, :application_deployment_name, :created_by)";

    String billingAccountId =
        Optional.ofNullable(profileRequest.getBillingAccountId()).orElse(null);
    String cloudPlatform =
        Optional.ofNullable(profileRequest.getCloudPlatform())
            .or(() -> Optional.of(CloudPlatform.GCP))
            .map(Enum::name)
            .get();
    UUID tenantId = profileRequest.getTenantId();
    UUID subscriptionId = profileRequest.getSubscriptionId();
    String resourceGroupName =
        Optional.ofNullable(profileRequest.getResourceGroupName()).orElse(null);
    String applicationDeploymentName =
        Optional.ofNullable(profileRequest.getApplicationDeploymentName()).orElse(null);

    MapSqlParameterSource params =
        new MapSqlParameterSource()
            .addValue("id", profileRequest.getId())
            .addValue("name", profileRequest.getProfileName())
            .addValue("biller", profileRequest.getBiller())
            .addValue("billing_account_id", billingAccountId)
            .addValue("description", profileRequest.getDescription())
            .addValue("cloud_platform", cloudPlatform)
            .addValue("tenant_id", tenantId)
            .addValue("subscription_id", subscriptionId)
            .addValue("resource_group_name", resourceGroupName)
            .addValue("application_deployment_name", applicationDeploymentName)
            .addValue("created_by", creator);

    DaoKeyHolder keyHolder = new DaoKeyHolder();
    jdbcTemplate.update(sql, params, keyHolder);

    return new BillingProfileModel()
        .id(keyHolder.getId())
        .profileName(keyHolder.getString("name"))
        .biller(keyHolder.getString("biller"))
        .billingAccountId(keyHolder.getString("billing_account_id"))
        .description(keyHolder.getString("description"))
        .cloudPlatform(CloudPlatform.valueOf(keyHolder.getString("cloud_platform")))
        .tenantId(keyHolder.getField("tenant_id", UUID.class).orElse(null))
        .subscriptionId(keyHolder.getField("subscription_id", UUID.class).orElse(null))
        .resourceGroupName(keyHolder.getString("resource_group_name"))
        .applicationDeploymentName(keyHolder.getString("application_deployment_name"))
        .createdBy(keyHolder.getString("created_by"))
        .createdDate(keyHolder.getTimestamp("created_date").toInstant().toString());
  }

  @Transactional(propagation = Propagation.REQUIRED, isolation = Isolation.SERIALIZABLE)
  public BillingProfileModel updateBillingProfileById(BillingProfileUpdateModel profileRequest) {
    String sql =
        "UPDATE billing_profile "
            + "SET billing_account_id = :billing_account_id, description = :description "
            + "WHERE id = :id";
    MapSqlParameterSource params =
        new MapSqlParameterSource()
            .addValue("id", profileRequest.getId())
            .addValue("billing_account_id", profileRequest.getBillingAccountId())
            .addValue("description", profileRequest.getDescription());
    DaoKeyHolder keyHolder = new DaoKeyHolder();
    int updated = jdbcTemplate.update(sql, params, keyHolder);

    // Assume if the following two conditions are true, then the profile could not be found
    // 1. the db command successfully completed
    // 2. no rows were updated
    if (updated != 1) {
      throw new ProfileNotFoundException("Billing Profile was not updated.");
    }

    return new BillingProfileModel()
        .id(keyHolder.getId())
        .profileName(keyHolder.getString("name"))
        .biller(keyHolder.getString("biller"))
        .billingAccountId(keyHolder.getString("billing_account_id"))
        .description(keyHolder.getString("description"))
        .cloudPlatform(CloudPlatform.valueOf(keyHolder.getString("cloud_platform")))
        .tenantId(keyHolder.getField("tenant_id", UUID.class).orElse(null))
        .subscriptionId(keyHolder.getField("subscription_id", UUID.class).orElse(null))
        .resourceGroupName(keyHolder.getString("resource_group_name"))
        .applicationDeploymentName(keyHolder.getString("application_deployment_name"))
        .createdBy(keyHolder.getString("created_by"))
        .createdDate(keyHolder.getTimestamp("created_date").toInstant().toString());
  }

  @Transactional(propagation = Propagation.REQUIRED, readOnly = true)
  public EnumerateBillingProfileModel enumerateBillingProfiles(
      int offset, int limit, Collection<UUID> accessibleProfileId) {

    MapSqlParameterSource params =
        new MapSqlParameterSource()
            .addValue("offset", offset)
            .addValue("limit", limit)
            .addValue("idlist", accessibleProfileId);

    List<BillingProfileModel> profiles =
        jdbcTemplate.query(SQL_LIST, params, new BillingProfileMapper());
    Integer total = jdbcTemplate.queryForObject(SQL_TOTAL, params, Integer.class);
    if (total == null) {
      throw new CorruptMetadataException("Impossible null value from count");
    }

    return new EnumerateBillingProfileModel().items(profiles).total(total);
  }

  @Transactional(propagation = Propagation.REQUIRED, readOnly = true)
  public BillingProfileModel getBillingProfileById(UUID id) {
    try {
      MapSqlParameterSource params = new MapSqlParameterSource().addValue("id", id);
      return jdbcTemplate.queryForObject(SQL_GET, params, new BillingProfileMapper());
    } catch (EmptyResultDataAccessException ex) {
      throw new ProfileNotFoundException("Profile not found for id: " + id.toString());
    }
  }

  @Transactional(propagation = Propagation.REQUIRED, isolation = Isolation.SERIALIZABLE)
  public boolean deleteBillingProfileById(UUID id) {
    try {
      int rowsAffected =
          jdbcTemplate.update(
              "DELETE FROM billing_profile WHERE id = :id",
              new MapSqlParameterSource().addValue("id", id));
      return rowsAffected > 0;
    } catch (DataIntegrityViolationException ex) {
      // Just in case some concurrent thing slips through the usage check step,
      // handle a case of some active references.
      throw new ProfileInUseException("Profile is in use and cannot be deleted", ex);
    }
  }

  /**
   * This method is made for use by upgrade, where we need to find all of the old billing profiles
   * without regard to visibility.
   *
   * @return list of billing profile models
   */
  @Transactional(propagation = Propagation.REQUIRED, readOnly = true)
  public List<BillingProfileModel> getOldBillingProfiles() {
    return jdbcTemplate.query(SQL_LIST_ALL, new BillingProfileMapper());
  }

  private static class BillingProfileMapper implements RowMapper<BillingProfileModel> {
    public BillingProfileModel mapRow(ResultSet rs, int rowNum) throws SQLException {
      return new BillingProfileModel()
          .id(rs.getObject("id", UUID.class))
          .profileName(rs.getString("name"))
          .biller(rs.getString("biller"))
          .billingAccountId(rs.getString("billing_account_id"))
          .description(rs.getString("description"))
          .cloudPlatform(CloudPlatform.valueOf(rs.getString("cloud_platform")))
          .tenantId(rs.getObject("tenant_id", UUID.class))
          .subscriptionId(rs.getObject("subscription_id", UUID.class))
          .resourceGroupName(rs.getString("resource_group_name"))
          .applicationDeploymentName(rs.getString("application_deployment_name"))
          .createdDate(rs.getTimestamp("created_date").toInstant().toString())
          .createdBy(rs.getString("created_by"));
    }
  }
}
