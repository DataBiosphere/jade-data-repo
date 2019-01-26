package bio.terra.configuration;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;

@Component
@EnableConfigurationProperties
@ConfigurationProperties(prefix = "db.datarepo")
public class DataRepoJdbcConfiguration extends JdbcConfiguration {

    @Bean
    public DataSource dataSource() {
        return super.getDataSource();
    }
}
