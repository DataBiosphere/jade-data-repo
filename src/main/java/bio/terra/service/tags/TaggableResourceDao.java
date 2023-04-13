package bio.terra.service.tags;

import bio.terra.common.DaoUtils;
import bio.terra.model.TagCount;
import java.sql.Array;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import javax.sql.DataSource;
import org.apache.commons.lang3.StringUtils;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/** An interface to share resource-agnostic dao operations related to user-defined tags. */
public interface TaggableResourceDao {

  NamedParameterJdbcTemplate getJdbcTemplate();

  DataSource getJdbcDataSource();

  String getTable();

  /**
   * @param ids accessible resource UUIDs
   * @param filter filter to tags with this string as a case-insensitive match
   * @param limit maximum number of tags to return, or all if unspecified
   * @return accessible resource tags and their occurrence counts matching specified filters
   */
  @Transactional(
      readOnly = true,
      propagation = Propagation.REQUIRED,
      isolation = Isolation.SERIALIZABLE)
  default List<TagCount> getTags(Collection<UUID> ids, String filter, Integer limit) {
    MapSqlParameterSource params = new MapSqlParameterSource();
    List<String> whereClauses = new ArrayList<>();
    DaoUtils.addAuthzIdsClause(ids, params, whereClauses, getTable());

    String sql =
        """
            SELECT * FROM
              (SELECT UNNEST(tags) AS tag, COUNT(*) AS count
                FROM %s
                WHERE %s
                GROUP BY tag
                ORDER BY count DESC, tag ASC) unfiltered
            """
            .formatted(getTable(), StringUtils.join(whereClauses, " AND "));

    if (filter != null) {
      sql += " WHERE tag ILIKE :filter";
      params.addValue("filter", DaoUtils.escapeFilter(filter));
    }
    if (limit != null) {
      sql += " LIMIT :limit";
      params.addValue("limit", limit);
    }

    return getJdbcTemplate().query(sql, params, new TagCountMapper());
  }

  class TagCountMapper implements RowMapper<TagCount> {
    public TagCount mapRow(ResultSet rs, int rowNum) throws SQLException {
      return new TagCount().tag(rs.getString("tag")).count(rs.getInt("count"));
    }
  }

  /**
   * @param id resource UUID
   * @param add tags to add to the resource
   * @param remove tags to remove from the resource
   * @return whether the resource record was updated
   */
  @Transactional(propagation = Propagation.REQUIRED, isolation = Isolation.SERIALIZABLE)
  default boolean updateTags(UUID id, List<String> add, List<String> remove) {
    String sql =
        """
        UPDATE %s SET tags =
          (SELECT ARRAY_AGG(tag) FROM
            (SELECT UNNEST(tags) tag                  -- Start with existing tags
              UNION SELECT UNNEST(:tagsToAdd) tag     -- Include tags to add
              EXCEPT SELECT UNNEST(:tagsToRemove) tag -- Exclude tags to remove
              ORDER BY tag ASC) updated               -- Set operations may not preserve order
          WHERE tag IS NOT NULL)
        WHERE id = :id
        """
            .formatted(getTable());

    Array tagsToAdd;
    Array tagsToRemove;
    try (Connection connection = getJdbcDataSource().getConnection()) {
      tagsToAdd = DaoUtils.createSqlStringArray(connection, add);
      tagsToRemove = DaoUtils.createSqlStringArray(connection, remove);
    } catch (SQLException e) {
      throw new IllegalArgumentException("Failed to convert tag update lists to SQL arrays", e);
    }

    MapSqlParameterSource params =
        new MapSqlParameterSource()
            .addValue("tagsToAdd", tagsToAdd)
            .addValue("tagsToRemove", tagsToRemove)
            .addValue("id", id);

    int rowsAffected = getJdbcTemplate().update(sql, params);
    return (rowsAffected == 1);
  }
}
