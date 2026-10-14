package bio.terra.app.configuration;

import bio.terra.app.logging.LoggerInterceptor;
import bio.terra.app.usermetrics.UserMetricsInterceptor;
import bio.terra.service.configuration.exception.ConfigException;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.PathMatchConfigurer;
import org.springframework.web.servlet.config.annotation.ResourceHandlerRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;
import org.springframework.web.util.UrlPathHelper;

@Component
public class WebConfig implements WebMvcConfigurer {
  @Autowired private LoggerInterceptor loggerInterceptor;
  @Autowired private UserMetricsInterceptor metricsInterceptor;

  public static final String SWAGGER_UI_VERSION = getSwaggerUiVersion();

  @Override
  public void addInterceptors(InterceptorRegistry registry) {
    registry.addInterceptor(loggerInterceptor);
    // Temporarily disabled due to thread pool exhaustion under high load
    // registry.addInterceptor(metricsInterceptor);
  }

  @Override
  public void configurePathMatch(PathMatchConfigurer configurer) {
    // This override is needed in order to allow encoded slashes in the path of a URL.
    UrlPathHelper urlPathHelper = new UrlPathHelper();
    // By setting this to false, Spring does not decode the path before matching it to a
    // method.  Rather, it does it after matching so that by the time the value reaches the
    // controller function, the value is decoded.
    urlPathHelper.setUrlDecode(false);
    configurer.setUrlPathHelper(urlPathHelper);
  }

  @Override
  public void addResourceHandlers(ResourceHandlerRegistry registry) {
    registry
        .addResourceHandler("/webjars/swagger-ui-dist/**")
        .addResourceLocations(
            String.format(
                "classpath:/META-INF/resources/webjars/swagger-ui-dist/%s/", SWAGGER_UI_VERSION));
  }

  protected static String getSwaggerUiVersion() {
    try {
      PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
      Resource[] resources =
          resolver.getResources("classpath:/META-INF/resources/webjars/swagger-ui-dist/*/");
      Pattern versionPattern = Pattern.compile(".+/swagger-ui-dist/(\\d+\\.\\d+\\.\\d+)/");
      Matcher matcher = versionPattern.matcher(((ClassPathResource) resources[0]).getPath());
      Optional<String> currentVersion = matcher.results().findFirst().map(m -> m.group(1));
      return currentVersion.orElseThrow();
    } catch (Exception e) {
      throw new ConfigException(
          "Failed to determine Swagger UI version from classpath resources", e);
    }
  }
}
