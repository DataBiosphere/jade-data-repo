package bio.terra.app.configuration;

import bio.terra.app.logging.LoggerInterceptor;
import bio.terra.app.usermetrics.UserMetricsInterceptor;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.PathMatchConfigurer;
import org.springframework.web.servlet.config.annotation.ResourceHandlerRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;
import org.springframework.web.util.UrlPathHelper;

@Component
public class WebConfig implements WebMvcConfigurer {
  private final Logger logger = LoggerFactory.getLogger(WebConfig.class);

  @Autowired private LoggerInterceptor loggerInterceptor;
  @Autowired private UserMetricsInterceptor metricsInterceptor;

  @Override
  public void addInterceptors(InterceptorRegistry registry) {
    registry.addInterceptor(loggerInterceptor);
    registry.addInterceptor(metricsInterceptor);
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
    Properties properties = new Properties();
    try (InputStream propsFile =
        getClass().getClassLoader().getResourceAsStream("swagger-ui.properties")) {
      properties.load(propsFile);
    } catch (IOException e) {
      logger.warn("Could not access project.properties file, using defaults");
    }
    String swaggerUIVersion = String.valueOf(properties.get("swagger-ui"));
    registry
        .addResourceHandler("/webjars/swagger-ui-dist/**")
        .addResourceLocations(
            "classpath:/META-INF/resources/webjars/swagger-ui-dist/%s/"
                .formatted(swaggerUIVersion));
  }
}
