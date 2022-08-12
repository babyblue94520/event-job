# Event Job

## Cause

想在後臺管理系統介面化管理 **Quartz Job**，就必須將所有 **Job Class** 導入管理系統中，但也強迫系統成為 **Quartz Cluster** 節點之一，導致伺服器資源被 **Job**
占用，所以開發一套基於事件綁定的排程任務管理器，解決任務管理和任務執行的相依性

## Goal

* Separate job managers and executors on different project

## Requirement

* Spring Boot 2+
* Java 11+

## Overview

![](./images/event_job.png)
![](./images/job_running.png)

## QuickStart

### pom.xml

```xml

<dependency>
    <groupId>io.github.babyblue94520</groupId>
    <artifactId>event-job</artifactId>
    <version>0.0.1-SNAPSHOT</version>
</dependency>
```

### yaml

```yaml
event-job:
  instance: eventJobScheduler
  topic: event.job
  reload-interval: 60000
  thread-count: 20 # default processors * 2
  check-wait-time: 1000 # Wait time to check if a job is being processed
```

### Config

```java

@EnableEventJob
public class EventJobConfig {

}
```

### Create or modify job

```java
@EnableEventJob
public class EventJobConfig {

    @Autowired
    private EventScheduler eventScheduler;

    @Override
    public void afterPropertiesSet() throws Exception {
        eventScheduler.add(EventJob.builder()
                .group("group")
                .name("name")
                .event("event")
                .cron("*/1 * * * * ?")
                .timezone("+00:00")
                .build());
    }
}
```

### Register job executor

```java
@EnableEventJob
public class EventJobRegister implements InitializingBean{

  @Autowired
  private EventScheduler eventScheduler;
  
  @Override
  public void afterPropertiesSet() throws Exception {
      eventScheduler.addHandler(eventJob.getEvent(), (eventJob2) -> {
          // Do something
      });
  }
}
```

### Add MessageService

* Notify jobs changes immediately

**Example**

```java
@Log4j2
@Service
public class EventJobMessageServiceImpl implements EventJobMessageService {

    private static final Map<String, List<Consumer<String>>> topicListenerMap = new ConcurrentHashMap<>();
    private static final ExecutorService executor = Executors.newFixedThreadPool(1);

    @Override
    public Runnable onConnected(Runnable runnable) {
        return null;
    }

    @Override
    public String send(String topic, String body) {
        executor.submit(() -> {
            topicListenerMap.getOrDefault(topic, Collections.emptyList()).forEach(consumer -> consumer.accept(body));
        });
        return body;
    }

    @Override
    public Consumer<String> addListener(String topic, Consumer<String> listener) {
        topicListenerMap.computeIfAbsent(topic, (key) -> new CopyOnWriteArrayList<>()).add(listener);
        return listener;
    }
}
```

### Create a job that depends on a job

```java
@EnableEventJob
public class EventJobConfig {

    @Autowired
    private EventScheduler eventScheduler;

    @Override
    public void afterPropertiesSet() throws Exception {
        // Execute after 'group-name' job completes.
        eventScheduler.add(EventJob.builder()
                .group("after-group")
                .name("after-name")
                .event("after-event")
                .afterGroup("group")
                .afterName("name")
                .timezone("+00:00")
                .build());
    }
}
```
