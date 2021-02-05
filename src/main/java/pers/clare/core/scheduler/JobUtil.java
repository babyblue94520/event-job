package pers.clare.core.scheduler;

import org.springframework.scheduling.support.CronSequenceGenerator;

import java.time.ZoneId;
import java.util.Date;
import java.util.TimeZone;

public class JobUtil {
    JobUtil() {
    }

    public static CronSequenceGenerator buildCronGenerator(String cron, String timezone) {
        if (timezone == null) {
            return new CronSequenceGenerator(cron);
        } else {
            return new CronSequenceGenerator(cron, TimeZone.getTimeZone(ZoneId.of(timezone)));
        }
    }


    public static long getNextTime(CronSequenceGenerator cronSequenceGenerator) {
        return cronSequenceGenerator.next(new Date()).getTime();
    }
}
