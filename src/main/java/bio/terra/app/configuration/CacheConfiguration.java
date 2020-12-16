package bio.terra.app.configuration;

import com.google.common.cache.CacheBuilder;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.CachingConfigurer;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.cache.concurrent.ConcurrentMapCache;
import org.springframework.cache.concurrent.ConcurrentMapCacheManager;
import org.springframework.cache.interceptor.CacheErrorHandler;
import org.springframework.cache.interceptor.CacheResolver;
import org.springframework.cache.interceptor.KeyGenerator;
import org.springframework.cache.interceptor.SimpleCacheErrorHandler;
import org.springframework.cache.interceptor.SimpleCacheResolver;
import org.springframework.cache.interceptor.SimpleKeyGenerator;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.lang.NonNull;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * Configures a cache to be used by Spring.  It likely makes sense to use a cache per endpoint.
 */
@EnableCaching
@Configuration
public class CacheConfiguration implements CachingConfigurer {

    public static final String SPRING_CACHE_MANAGER = "springCacheManager";

    // Use these strings to reference CacheProperties
    public static final String STATUS_PROP = "STATUS";

    public enum CacheProperties {
        STATUS(30L, TimeUnit.SECONDS, null);

        private final long duration;
        private final TimeUnit unit;
        private final Optional<Long> maxSize;

        CacheProperties(Long duration, TimeUnit unit, Long maxSize) {
            this.duration = duration;
            this.unit = unit;
            this.maxSize = Optional.ofNullable(maxSize);
        }
    };

    @Bean(name = SPRING_CACHE_MANAGER)
    @Override
    public CacheManager cacheManager() {
        return new ConcurrentMapCacheManager() {

            @Override
            @NonNull
            protected Cache createConcurrentMapCache(@NonNull final String name) {
                final CacheProperties config = CacheProperties.valueOf(name);
                final CacheBuilder<Object, Object> builder = CacheBuilder.newBuilder()
                    .expireAfterWrite(config.duration, config.unit);

                config.maxSize.ifPresent(builder::maximumSize);

                return new ConcurrentMapCache(name, builder.build().asMap(), false);
            }
        };
    }

    @Override
    public CacheResolver cacheResolver() {
        return new SimpleCacheResolver();
    }

    @Override
    public KeyGenerator keyGenerator() {
        return new SimpleKeyGenerator();
    }

    @Override
    public CacheErrorHandler errorHandler() {
        return new SimpleCacheErrorHandler();
    }

}
