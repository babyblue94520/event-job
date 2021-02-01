package pers.clare.redis;

import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;

public class MyRedisMessageListenerContainer extends RedisMessageListenerContainer {

    /**
     * 包装成RedisConnectionFailureException 让重新连线动作正常
     * @param ex
     */
    @Override
    protected void handleSubscriptionException(Throwable ex) {
        super.handleSubscriptionException(new RedisConnectionFailureException(ex.getMessage(),ex));
    }
}
