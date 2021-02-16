package pers.clare.redis;

import lombok.extern.log4j.Log4j2;
import org.springframework.data.redis.connection.*;
import org.springframework.util.StringUtils;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;


@Log4j2
public class RedisSubListenerContainer {
    private final static Charset charset = StandardCharsets.UTF_8;
    private final static long DELAY = 5000;
    private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(Runtime.getRuntime().availableProcessors());

    private final Map<String, Collection<MessageListener>> patternMap = new ConcurrentHashMap<>();

    private final Map<String, Collection<MessageListener>> channelMap = new ConcurrentHashMap<>();

    private RedisConnectionFactory connectionFactory;

    private RedisConnection connection;

    private ScheduledFuture connectionFuture;

    public RedisSubListenerContainer(RedisConnectionFactory connectionFactory) {
        this.connectionFactory = connectionFactory;
    }

    public MessageListener addMessageListener(MessageListener messageListener, String channel) {
        return addMessageListener(true, channelMap, messageListener, channel);
    }

    public MessageListener addPatternMessageListener(MessageListener messageListener, String pattern) {
        return addMessageListener(false, patternMap, messageListener, pattern);
    }

    public boolean removeMessageListener(MessageListener messageListener, String channel) {
        return removeMessageListener(true, channelMap, messageListener, channel);
    }

    public boolean removePatternMessageListener(MessageListener messageListener, String pattern) {
        return removeMessageListener(false, patternMap, messageListener, pattern);
    }

    private MessageListener addMessageListener(
            boolean channel
            , Map<String, Collection<MessageListener>> target
            , MessageListener messageListener
            , String topic
    ) {
        synchronized (this) {
            if (StringUtils.isEmpty(topic)) return messageListener;
            Collection<MessageListener> messageListeners = target.get(topic);
            if (messageListeners == null) {
                messageListeners = target.get(topic);
                if (messageListeners == null) {
                    target.put(topic, messageListeners = new ArrayList<>());
                }
            }
            if (messageListeners.contains(messageListener)) return messageListener;
            messageListeners.add(messageListener);
        }
        listen(channel, topic);
        return messageListener;
    }


    private boolean removeMessageListener(
            boolean channel
            , Map<String, Collection<MessageListener>> target
            , MessageListener messageListener
            , String topic
    ) {
        if (connection == null || !connection.isSubscribed()) return false;
        synchronized (this) {
            if (StringUtils.isEmpty(topic)) return false;
            Collection<MessageListener> messageListeners = target.get(topic);
            if (messageListeners == null) return false;
            if (messageListeners.remove(messageListener)) {
                if (channel) {
                    connection.getSubscription().unsubscribe(topic.getBytes(charset));
                } else {
                    connection.getSubscription().pUnsubscribe(topic.getBytes(charset));
                }
            }
            return false;
        }
    }

    private void listen(boolean channel, String topic) {
        if (connection == null) {
            synchronized (this) {
                if (connection == null) {
                    if (connectionFuture == null) {
                        connectionFuture = executor.schedule(this::connection, DELAY, TimeUnit.MILLISECONDS);
                    }
                    return;
                }
            }
        }
        if (channel) {
            connection.getSubscription().subscribe(topic.getBytes(charset));
        } else {
            connection.getSubscription().pSubscribe(topic.getBytes(charset));
        }

    }

    private void connection() {
        synchronized (this) {
            try {
                if (patternMap.isEmpty() && channelMap.isEmpty()) return;
                connection = connectionFactory.getConnection();
                if (channelMap.size() > 0) {
                    connection.subscribe(new SubMessageListener(), toBytes(channelMap.keySet()));
                }
                if (patternMap.size() > 0) {
                    if (connection.isSubscribed()) {
                        connection.getSubscription().pSubscribe(toBytes(patternMap.keySet()));
                    } else {
                        connection.pSubscribe(new SubMessageListener(), toBytes(patternMap.keySet()));
                    }
                }
                connectionFuture = null;
                log.info("redis connection success");
            } catch (Exception e) {
                log.error(e.getMessage(), e);
                log.warn("redis reconnection after {} milliseconds", DELAY);
                connection = null;
                connectionFuture = executor.schedule(this::connection, DELAY, TimeUnit.MILLISECONDS);
            }
        }
    }

    private void send(Message message, byte[] pattern, Collection<MessageListener> messageListeners) {
        if (messageListeners == null || messageListeners.size() == 0) return;
        Iterator<MessageListener> iterator = messageListeners.iterator();
        executor.execute(() -> {
            while (iterator.hasNext()) {
                try {
                    iterator.next().onMessage(message, pattern);
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                }
            }
        });
    }

    private byte[][] toBytes(Set<String> keys) {
        byte[][] bytes = new byte[keys.size()][];
        int i = 0;
        for (String key : keys) {
            bytes[i++] = key.getBytes(charset);
        }
        return bytes;
    }

    class SubMessageListener implements MessageListener {

        @Override
        public void onMessage(Message message, byte[] pattern) {
            if (pattern == null) {
                send(message, null, channelMap.get(new String(message.getChannel(), charset)));
            } else {
                send(message, pattern, patternMap.get(new String(pattern, charset)));
            }
        }
    }

}
