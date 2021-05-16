package pers.clare.eventjob.util;

import org.springframework.scheduling.support.CronSequenceGenerator;

import java.time.ZoneId;
import java.util.Date;
import java.util.TimeZone;

public class JobUtil {
    private JobUtil() {
    }

    public static long getNextTime(String cron, String timezone) {
        if (timezone == null) {
            return new CronSequenceGenerator(cron).next(new Date()).getTime();
        } else {
            return new CronSequenceGenerator(cron, TimeZone.getTimeZone(ZoneId.of(timezone))).next(new Date()).getTime();
        }
    }
}
