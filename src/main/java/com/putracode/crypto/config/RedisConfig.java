package com.putracode.crypto.config;

import io.github.dengliming.redismodule.redisjson.RedisJSON;
import io.github.dengliming.redismodule.redisjson.client.RedisJSONClient;
import io.github.dengliming.redismodule.redistimeseries.RedisTimeSeries;
import io.github.dengliming.redismodule.redistimeseries.client.RedisTimeSeriesClient;
import org.redisson.config.Config;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RedisConfig {
    private static final String redisURL
            ="redis://127.0.0.1:6379";
    @Bean
    public Config config(){
        Config config=new Config();
        config.useSingleServer().setAddress(redisURL).setPassword("root");
        return config;
    }
    @Bean
    public RedisTimeSeriesClient redisTimeSeriesClient(Config config){
        return new RedisTimeSeriesClient(config);
    }
    @Bean
    public RedisTimeSeries redisTimeSeries(RedisTimeSeriesClient re){
        return re.getRedisTimeSeries();
    }
    @Bean
    public RedisJSONClient redisJSONClient(Config config){
        return new RedisJSONClient(config);
    }
    @Bean
    public RedisJSON redisJSON(RedisJSONClient re){
        return re.getRedisJSON();
    }
}
