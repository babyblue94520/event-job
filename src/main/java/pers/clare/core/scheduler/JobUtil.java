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

    public static long getNextTime(String cron, String timezone) {
        if (timezone == null) {
            return new CronSequenceGenerator(cron).next(new Date()).getTime();
        } else {
            return new CronSequenceGenerator(cron, TimeZone.getTimeZone(ZoneId.of(timezone))).next(new Date()).getTime();
        }
    }

    public static void main(String[] args) {
        String cron = "0 50 11 * * ?";
        CronSequenceGenerator cronSequenceGenerator = buildCronGenerator(cron,"-14:00");
        System.out.println(cronSequenceGenerator.next(new Date()).getTime()-System.currentTimeMillis());
        cronSequenceGenerator = buildCronGenerator(cron,"+00:00");
        System.out.println(cronSequenceGenerator.next(new Date()).getTime()-System.currentTimeMillis());
        cronSequenceGenerator = buildCronGenerator(cron,"+08:00");
        System.out.println(cronSequenceGenerator.next(new Date()).getTime()-System.currentTimeMillis());
    }
}
