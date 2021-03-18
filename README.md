# 分散式事件排程任務

## 事由

想在後臺管理系統介面化管理 **Quartz Job**，就必須將所有 **Job Class** 導入管理系統中，但也強迫系統成為 **Quartz Cluster** 節點之一，導致伺服器資源被 **Job** 占用，所以開發一套基於事件綁定的排程任務管理器，解決任務管理和任務執行的相依性

## 目標

- 將**執行任務**和**管理任務**分開在不同專案上實作
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

一、配置 **Scheduler**
  
  * **任務管理專案**和**任務執行專案**都需要配置
  
    **一般**

    * 預設每**1**分鐘從資料庫查詢最新任務資訊

      ```java
      @Configuration
      public class ScheduleConfig {

        @Bean
        public Scheduler scheduler(
                @Value("${event-job.name}") String instance // 自訂義 schedule instance 名稱
                , @Qualifier("dataSource") DataSource dataSource // 持久化資料庫來源
        ) {
          return new SchedulerImpl(instance, dataSource);
        }
      }
      ```
  
    **進階**

      * 透過實作 **EventJobMessageService** 發布任務更新事件，以 **Redis** 為例
      * 預設每 **10** 分鐘從資料庫查詢最新任務資訊
      
        ```java
        @Configuration
        public class ScheduleConfig implements InitializingBean {

            @Bean
            public Scheduler scheduler(
                    @Value("${event-job.name}") String instance // 自訂義 schedule instance 名稱
                    , @Qualifier("dataSource") DataSource dataSource // 持久化資料庫來源
                    , @Value("${event-job.topic}") String topic // Topic
                    , EventJobMessageService eventJobMessageService // MessageService
            ) {
                return new SchedulerImpl(instance, dataSource, topic, eventJobMessageService);
            }
        }
        ```
      

二、新增任務
  
  * 僅需配置在**任務管理專案**中
  * 如果任務已存，則檢查是否有異動並更新任務

    ```java
    @Configuration
    public class ScheduleJobConfig implements InitializingBean {

      @Autowired
      private Scheduler scheduler;

      @Override
      public void afterPropertiesSet() throws Exception {
        // 新增任務
        Job job = new Job("group", "name", "description", "+00:00", "0/1 * * * * ?", true, null);
        scheduler.add(job);
      }
    }
    ```

三、註冊任務處理
  
  * 僅需配置在**任務執行專案**中
  * 同時可以配置在多個**任務執行專案**中，同一時間僅會有一個服務執行任務

    ```java
    @Component
    public class JobHandler implements InitializingBean {

      @Autowired
      private Scheduler scheduler;

      @Override
      public void afterPropertiesSet() throws Exception {
        // 註冊任務處理
        scheduler.register("group", "name", eventJob -> {
            // TODO
        });
      }
    }
    ```
