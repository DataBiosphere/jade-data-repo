package bio.terra.app.configuration;

import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.data.jdbc.JdbcRepositoriesAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceTransactionManagerAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.JdbcTemplateAutoConfiguration;
import org.springframework.boot.autoconfigure.transaction.TransactionAutoConfiguration;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

@EnableAutoConfiguration(
    exclude = {
      DataSourceAutoConfiguration.class,
      DataSourceTransactionManagerAutoConfiguration.class,
      JdbcTemplateAutoConfiguration.class,
      JdbcRepositoriesAutoConfiguration.class,
      TransactionAutoConfiguration.class
    })
@Configuration
@ConditionalOnProperty(
    prefix = "datarepo",
    name = "testWithEmbeddedDatabase",
    havingValue = "false")
public class TestApplicationConfiguration extends ApplicationConfiguration {

  @MockBean(name = "jdbcTemplate")
  public NamedParameterJdbcTemplate namedParameterJdbcTemplate;
}
