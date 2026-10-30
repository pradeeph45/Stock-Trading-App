package com.trading.marketdataservice.config;

import com.trading.marketdataservice.dto.StockPrice;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.JacksonJsonRedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;

@Configuration
public class RedisConfig {

    @Bean
    public RedisTemplate<String, Object> redisTemplate(
            RedisConnectionFactory connectionFactory) {

        RedisTemplate<String, Object> template =
                new RedisTemplate<>();

        template.setConnectionFactory(connectionFactory);

        template.setKeySerializer(
                new StringRedisSerializer()
        );

        template.setValueSerializer(
                new JacksonJsonRedisSerializer<>(
                        StockPrice.class
                )
        );

        return template;
    }
}
