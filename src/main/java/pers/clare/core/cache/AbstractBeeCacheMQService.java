package pers.clare.core.cache;

import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.InitializingBean;

import java.util.ArrayList;
import java.util.List;

@Log4j2
public abstract class AbstractBeeCacheMQService implements BeeCacheMQService, InitializingBean {

    private List<Runnable> connectedListeners = new ArrayList<>();

    @Override
    public void onConnected(Runnable runnable) {
        connectedListeners.add(runnable);
    }

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
