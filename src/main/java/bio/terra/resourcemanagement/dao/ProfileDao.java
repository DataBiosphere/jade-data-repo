package bio.terra.resourcemanagement.dao;

import bio.terra.dao.DaoKeyHolder;
import bio.terra.resourcemanagement.dao.exception.AccountAlreadyInUse;
import bio.terra.dao.exception.ProfileNotFoundException;
import bio.terra.metadata.BillingProfile;
import bio.terra.metadata.MetadataEnumeration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Repository
public class ProfileDao {
    private final NamedParameterJdbcTemplate jdbcTemplate;

    @Autowired
    public ProfileDao(NamedParameterJdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public UUID createBillingProfile(BillingProfile billingProfile) {
        try {
            String sql = "INSERT INTO billing_profile (name, biller, billing_account_id) VALUES " +
                "(:name, :biller, :billing_account_id)";
            MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("name", billingProfile.getName())
                .addValue("biller", billingProfile.getBiller())
                .addValue("billing_account_id", billingProfile.getBillingAccountId());
            DaoKeyHolder keyHolder = new DaoKeyHolder();
            jdbcTemplate.update(sql, params, keyHolder);
            return keyHolder.getId();
        } catch (DuplicateKeyException ex) {
            String billingAccountId = billingProfile.getBillingAccountId();
            List<String> details = new ArrayList<>();
            try {
                BillingProfile profile = getBillingProfileByAccount(billingAccountId);
                details.add("Account being used by profile with id: " + profile.getId());
            } catch (ProfileNotFoundException e) {
                details.add("Unable to find profile using this billing account. Something bad has happened.");
            }
            throw new AccountAlreadyInUse("Account in use: " + billingAccountId, ex, details);
        }
    }

    public MetadataEnumeration<BillingProfile> enumerateBillingProfiles(Integer offset, Integer limit) {
        String sql = "SELECT * FROM billing_profile OFFSET :offset LIMIT :limit";
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("offset", offset)
            .addValue("limit", limit);
        List<BillingProfile> profiles = jdbcTemplate.query(sql, params, new BillingProfileMapper());
        sql = "SELECT count(id) AS total FROM billing_profile";
        params = new MapSqlParameterSource();
        Integer total = jdbcTemplate.queryForObject(sql, params, Integer.class);
        return new MetadataEnumeration<BillingProfile>()
            .items(profiles)
            .total(total == null ? -1 : total);
    }

    public BillingProfile getBillingProfileById(UUID id) {
        try {
            String sql = "SELECT * FROM billing_profile WHERE id = :id";
            MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("id", id);
            return jdbcTemplate.queryForObject(sql, params, new BillingProfileMapper());
        } catch (EmptyResultDataAccessException ex) {
            throw new ProfileNotFoundException("Profile not found for id: " + id.toString());
        }
    }

    public BillingProfile getBillingProfileByAccount(String billingAccountId) {
        try {
            String sql = "SELECT * FROM billing_profile WHERE billing_account_id = :billing_account_id";
            MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("billing_account_id", billingAccountId);
            return jdbcTemplate.queryForObject(sql, params, new BillingProfileMapper());
        } catch (EmptyResultDataAccessException ex) {
            throw new ProfileNotFoundException("Profile not found for billing account: " + billingAccountId);
        }
    }

    public boolean deleteBillingProfileById(UUID id) {
        int rowsAffected = jdbcTemplate.update("DELETE FROM billing_profile WHERE id = :id",
            new MapSqlParameterSource().addValue("id", id));
        return rowsAffected > 0;
    }

    private static class BillingProfileMapper implements RowMapper<BillingProfile> {
        public BillingProfile mapRow(ResultSet rs, int rowNum) throws SQLException {
            return new BillingProfile()
                .id(rs.getObject("id", UUID.class))
                .name(rs.getString("name"))
                .biller(rs.getString("biller"))
                .billingAccountId(rs.getString("billing_account_id"));
        }
    }
}
