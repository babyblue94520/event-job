package pers.clare.eventjob;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.InitializingBean;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public abstract class AbstractEventJobMessageService implements EventJobMessageService, InitializingBean {
    private static final Logger log = LogManager.getLogger();

    private List<Runnable> connectedListeners = new CopyOnWriteArrayList<>();

    @Override
    public void onConnected(Runnable runnable) {
        connectedListeners.add(runnable);
    }

    /**
     * 連線成功
     */
    protected void publishConnectedEvent(){
        for (Runnable runnable : connectedListeners) {
            try {
                runnable.run();
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }
    }
}
