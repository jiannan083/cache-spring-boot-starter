package cn.bfay.cache.redis;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * RedisAutoConfiguration.
 *
 * @author wangjiannan
 * @since 2019/10/28
 */
@Slf4j
@Configuration
public class RedisAutoConfiguration {
    @Bean
    @ConditionalOnMissingBean//缺失时，初始化bean并添加到SpringIoc
    public RedisUtils redisUtils() {
        log.info(">>>The RedisUtils Not Found，Execute Create New Bean.");
        return new RedisUtils();
    }
}
