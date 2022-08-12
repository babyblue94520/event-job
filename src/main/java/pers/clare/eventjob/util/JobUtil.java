package pers.clare.eventjob.util;

import org.springframework.scheduling.support.CronExpression;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Objects;

public class JobUtil {
    private JobUtil() {
    }

    public static long getNextTime(String cron, String timezone) {
        ZonedDateTime dateTime = getDateTime(timezone);
        return getNextTime(cron, dateTime);
    }

    public static long getNextDelay(String cron, String timezone) {
        return getNextTime(cron, timezone) - System.currentTimeMillis();
    }

    public static long getNextTime(String cron, ZonedDateTime timezone) {
        return Objects.requireNonNull(CronExpression.parse(cron).next(timezone)).toInstant().toEpochMilli();
    }

    public static ZonedDateTime getDateTime(String timezone) {
        return timezone == null ? ZonedDateTime.now() : ZonedDateTime.now(ZoneId.of(timezone));
    }

    public static void main(String[] args) {
        String cron = "0 0 0 * * ?";
        long nextTime, now = System.currentTimeMillis();

        nextTime = getNextTime(cron, "+00:00");
        System.out.println(nextTime + " " + (nextTime - now));
        nextTime = getNextTime(cron, "+08:00");
        System.out.println(nextTime + " " + (nextTime - now));
        nextTime = getNextTime(cron, "+04:00");
        System.out.println(nextTime + " " + (nextTime - now));
    }
}
