package bio.terra.dao;

import bio.terra.metadata.BillingProfile;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.UUID;

@Repository
public class ResourceDao {
    private final NamedParameterJdbcTemplate jdbcTemplate;

    @Autowired
    public ResourceDao(NamedParameterJdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public UUID createBillingProfile(BillingProfile billingProfile) {
        String sql = "INSERT INTO billing_profile (name, biller, billing_account_id) VALUES " +
            "(:name, :biller, :billing_account_id)";
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("name", billingProfile.getName())
            .addValue("biller", billingProfile.getBiller())
            .addValue("billing_account_id", billingProfile.getBillingAccountId());
        DaoKeyHolder keyHolder = new DaoKeyHolder();
        jdbcTemplate.update(sql, params, keyHolder);
        return keyHolder.getId();
    }

}
