package bio.terra.service.profile;

import bio.terra.common.DaoKeyHolder;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.BillingProfileRequestModel;
import bio.terra.model.EnumerateBillingProfileModel;
import bio.terra.service.profile.exception.ProfileNotFoundException;
import bio.terra.service.snapshot.exception.CorruptMetadataException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.UUID;

@Repository
public class ProfileDao {
    private final NamedParameterJdbcTemplate jdbcTemplate;

    // SQL select string constants
    private static final String sqlSelectList =
        "id, name, biller, billing_account_id, description, created_date, created_by";

    private static final String sqlGet = "SELECT " + sqlSelectList
        + " FROM billing_profile WHERE id = :id";

    private static final String sqlList = "SELECT " + sqlSelectList
        + " FROM billing_profile"
        + " WHERE id in (:idlist)"
        + " OFFSET :offset LIMIT :limit";

    private static final String sqlTotal = "SELECT count(id) AS total"
        + " FROM billing_profile"
        + " WHERE id in (:idlist)";

    private static final String sqlListAll = "SELECT " + sqlSelectList
        + " FROM billing_profile";

    @Autowired
    public ProfileDao(NamedParameterJdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Transactional(propagation = Propagation.REQUIRED, isolation = Isolation.SERIALIZABLE)
    public BillingProfileModel createBillingProfile(BillingProfileRequestModel profileRequest, String creator) {
        String sql = "INSERT INTO billing_profile"
            + " (id, name, biller, billing_account_id, description, created_by) VALUES "
            + " (:id, :name, :biller, :billing_account_id, :description, :created_by)";
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("id", UUID.fromString(profileRequest.getId()))
            .addValue("name", profileRequest.getProfileName())
            .addValue("biller", profileRequest.getBiller())
            .addValue("billing_account_id", profileRequest.getBillingAccountId())
            .addValue("description", profileRequest.getDescription())
            .addValue("created_by", creator);
        DaoKeyHolder keyHolder = new DaoKeyHolder();
        jdbcTemplate.update(sql, params, keyHolder);

        return new BillingProfileModel()
            .id(keyHolder.getId().toString())
            .profileName(keyHolder.getString("name"))
            .biller(keyHolder.getString("biller"))
            .billingAccountId(keyHolder.getString("billing_account_id"))
            .description(keyHolder.getString("description"))
            .createdBy(keyHolder.getString("created_by"))
            .createdDate(keyHolder.getTimestamp("created_date").toInstant().toString());
    }

    @Transactional(propagation = Propagation.REQUIRED, isolation = Isolation.SERIALIZABLE)
    public BillingProfileModel updateBillingProfileById(BillingProfileRequestModel profileRequest) {
        String sql = "UPDATE billing_profile "
            + "SET name = :name, biller = :biller, billing_account_id = :billing_account_id, "
            + "description = :description "
            + "WHERE id = :id";
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("id", UUID.fromString(profileRequest.getId()))
            .addValue("name", profileRequest.getProfileName())
            .addValue("biller", profileRequest.getBiller())
            .addValue("billing_account_id", profileRequest.getBillingAccountId())
            .addValue("description", profileRequest.getDescription());
        DaoKeyHolder keyHolder = new DaoKeyHolder();
        jdbcTemplate.update(sql, params, keyHolder);

        return new BillingProfileModel()
            .id(keyHolder.getId().toString())
            .profileName(keyHolder.getString("name"))
            .biller(keyHolder.getString("biller"))
            .billingAccountId(keyHolder.getString("billing_account_id"))
            .description(keyHolder.getString("description"))
            .createdBy(keyHolder.getString("created_by"))
            .createdDate(keyHolder.getTimestamp("created_date").toInstant().toString());
    }



    @Transactional(propagation = Propagation.REQUIRED, readOnly = true)
    public EnumerateBillingProfileModel enumerateBillingProfiles(
        int offset,
        int limit,
        List<UUID> accessibleProfileId) {

        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("offset", offset)
            .addValue("limit", limit)
            .addValue("idlist", accessibleProfileId);

        List<BillingProfileModel> profiles = jdbcTemplate.query(sqlList, params, new BillingProfileMapper());
        Integer total = jdbcTemplate.queryForObject(sqlTotal, params, Integer.class);
        if (total == null) {
            throw new CorruptMetadataException("Impossible null value from count");
        }

        return new EnumerateBillingProfileModel()
            .items(profiles)
            .total(total);
    }

    @Transactional(propagation = Propagation.REQUIRED, readOnly = true)
    public BillingProfileModel getBillingProfileById(UUID id) {
        try {
            MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("id", id);
            return jdbcTemplate.queryForObject(sqlGet, params, new BillingProfileMapper());
        } catch (EmptyResultDataAccessException ex) {
            throw new ProfileNotFoundException("Profile not found for id: " + id.toString());
        }
    }

    @Transactional(propagation = Propagation.REQUIRED, isolation = Isolation.SERIALIZABLE)
    public boolean deleteBillingProfileById(UUID id) {
        int rowsAffected = jdbcTemplate.update("DELETE FROM billing_profile WHERE id = :id",
            new MapSqlParameterSource().addValue("id", id));
        return rowsAffected > 0;
    }

    /**
     * This method is made for use by upgrade, where we need to find all of the old billing profiles
     * without regard to visibility.
     * @return list of billing profile models
     */
    @Transactional(propagation = Propagation.REQUIRED, readOnly = true)
    public List<BillingProfileModel> getOldBillingProfiles() {
        return jdbcTemplate.query(sqlListAll, new BillingProfileMapper());
    }

    private static class BillingProfileMapper implements RowMapper<BillingProfileModel> {
        public BillingProfileModel mapRow(ResultSet rs, int rowNum) throws SQLException {
            String profileId = rs.getObject("id", UUID.class).toString();
            return new BillingProfileModel()
                .id(profileId)
                .profileName(rs.getString("name"))
                .biller(rs.getString("biller"))
                .billingAccountId(rs.getString("billing_account_id"))
                .description(rs.getString("description"))
                .createdDate(rs.getTimestamp("created_date").toInstant().toString())
                .createdBy(rs.getString("created_by"));
        }
    }
}
