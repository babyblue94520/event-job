# 分散式事件排程任務

## 事由

想在後臺管理系統介面化管理 **Quartz Job**，就必須將所有 **Job Class** 導入管理系統中，但也強迫系統成為 **Quartz Cluster** 節點之一，導致伺服器資源被 **Job** 占用，所以開發一套基於事件綁定的排程任務管理器，解決任務管理和任務執行的相依性

## 目標

- 將執行任務和管理任務的分開在不同專案上實作
- 
- 避免網路 __I/O__ 和資料結構轉換的消耗，提高效能
- 相容 __Spring Cacheable__


## 設計

- 使用 **RDBS** 做任務持久化
- 使用 **Message Queue Service** 發布任務更新
- 移除參考 **Java Class** 為執行對象
- 以 **job group、job name** 註冊任務處理器

## 使用

### 任務管理服務

* 配置排程任務管理器
* 新增任務，如果任務已存，則檢查是否有異動並更新任務


```java
@Configuration
public class ScheduleConfig implements InitializingBean {

  @Autowired
  private Scheduler scheduler;

  @Override
  public void afterPropertiesSet() throws Exception {
    // 新增任務
    Job job = new Job("Test", "test", "test", "+00:00", "0/1 * * * * ?", true, null);
    scheduler.add(job);
  }

  @Bean
  public Scheduler scheduler(
          @Value("${event-job.name}") String instance
          , @Value("${event-job.topic}") String topic
          , @Qualifier("dataSource") DataSource dataSource
          , EventJobMessageService eventJobMessageService
  ) {
    return new SchedulerImpl(instance, dataSource, topic, eventJobMessageService);
  }
}
```

