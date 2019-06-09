package bio.terra.dao;

import bio.terra.metadata.BillingProfile;
import bio.terra.metadata.MetadataEnumeration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
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
