package org.cinchapi.concourse.time;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Unit tests for the {@link StringToTime} service.
 * 
 * @author Jeff Nelson
 */
public class StringToTimeTest {

    @Test
    public void testMySqlDateFormat() {
        Calendar cal = Calendar.getInstance();
        cal.set(Calendar.MONTH, Calendar.OCTOBER);
        cal.set(Calendar.DATE, 26);
        cal.set(Calendar.YEAR, 1981);
        cal.set(Calendar.HOUR_OF_DAY, 15);
        cal.set(Calendar.MINUTE, 26);
        cal.set(Calendar.SECOND, 3);
        cal.set(Calendar.MILLISECOND, 435);

        Assert.assertEquals(new Date(cal.getTimeInMillis()), StringToTime
                .parseDateTime("1981-10-26 15:26:03.435").toDate());
    }

    /*
     * FIXME
     * 
     * @Test public void testISO8601() {
     * Date now = new Date();
     * Calendar cal = Calendar.getInstance();
     * cal.setTime(now);
     * 
     * cal.set(Calendar.MONTH, Calendar.OCTOBER);
     * cal.set(Calendar.DATE, 26);
     * cal.set(Calendar.YEAR, 1981);
     * cal.set(Calendar.HOUR_OF_DAY, 15);
     * cal.set(Calendar.MINUTE, 25);
     * cal.set(Calendar.SECOND, 2);
     * cal.set(Calendar.MILLISECOND, 435);
     * 
     * Assert.assertEquals(new Date(cal.getTimeInMillis()), new
     * StringToTime("1981-10-26T15:26:03.435ZEST", now).toDate());
     * }
     */

    @Test
    public void test1200Seconds() {
        Date now = new Date();
        Calendar cal = Calendar.getInstance();
        cal.setTime(now);

        cal.set(Calendar.SECOND, cal.get(Calendar.SECOND) + 1200);
        Assert.assertTrue(new Date(cal.getTimeInMillis()).equals(StringToTime
                .parseDateTime("+1200 s", now).toDate()));
        Assert.assertFalse(new Date(cal.getTimeInMillis()).equals(StringToTime
                .parseDateTime("+1 s", now).toDate()));
    }

    @Test
    @Ignore
    public void testVariousExpressionsOfTimeOfDay() {
        Date now = new Date();
        Calendar cal = Calendar.getInstance();
        cal.setTime(now);

        cal.set(Calendar.HOUR_OF_DAY, 23);
        cal.set(Calendar.MINUTE, 59);
        cal.set(Calendar.SECOND, 59);
        cal.set(Calendar.MILLISECOND, 0);
        Assert.assertEquals(new Date(cal.getTimeInMillis()), StringToTime
                .parseDateTime("11:59:59 PM", now).toDate());
        Assert.assertEquals(new Date(cal.getTimeInMillis()), StringToTime
                .parseDateTime("23:59:59", now).toDate());

        cal.set(Calendar.SECOND, 0);
        Assert.assertEquals(new Date(cal.getTimeInMillis()), StringToTime
                .parseDateTime("23:59", now).toDate());
        Assert.assertEquals(new Date(cal.getTimeInMillis()), StringToTime
                .parseDateTime("11:59 PM", now).toDate());

        cal.set(Calendar.MILLISECOND, 123);
        Assert.assertEquals(new Date(cal.getTimeInMillis()),
                StringToTime.parseDateTime("23:59:00.123"));

        cal.set(Calendar.MONTH, Calendar.OCTOBER);
        cal.set(Calendar.DATE, 26);
        cal.set(Calendar.YEAR, 1981);
        cal.set(Calendar.HOUR_OF_DAY, 15);
        cal.set(Calendar.MINUTE, 27);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        Assert.assertEquals(new Date(cal.getTimeInMillis()), StringToTime
                .parseDateTime("October 26, 1981 3:27:00 PM", now).toDate());

        cal.set(Calendar.HOUR, 5);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.AM_PM, Calendar.PM);
        Assert.assertEquals(new Date(cal.getTimeInMillis()), StringToTime
                .parseDateTime("10/26/81 5PM", now).toDate());

        cal.setTime(now);
        cal.set(Calendar.DATE, cal.get(Calendar.DATE) + 1);
        cal.set(Calendar.HOUR, 5);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        cal.set(Calendar.AM_PM, Calendar.PM);
        Assert.assertEquals(new Date(cal.getTimeInMillis()), StringToTime
                .parseDateTime("tomorrow 5PM", now).toDate());

        cal.set(Calendar.DATE, cal.get(Calendar.DATE) - 2);
        Assert.assertEquals(new Date(cal.getTimeInMillis()), StringToTime
                .parseDateTime("yesterday 5PM", now).toDate());
        Assert.assertEquals(StringToTime
                .parseDateTime("yesterday evening", now).toDate(), StringToTime
                .parseDateTime("yesterday 5PM", now).toDate());
    }

    @Test
    public void testNow() {
        Date now = new Date();
        Assert.assertEquals(new Date(now.getTime()), StringToTime
                .parseDateTime("now", now).toDate());
    }

    @Test
    public void testToday() {
        Date now = new Date();
        Assert.assertEquals(StringToTime.parseDateTime("00:00:00.000", now)
                .toDate(), StringToTime.parseDateTime("today", now).toDate());
    }

    @Test
    public void testThisMorning() {
        Date now = new Date();
        Assert.assertEquals(StringToTime.parseDateTime("07:00:00.000", now)
                .toDate(), StringToTime.parseDateTime("this morning", now)
                .toDate());
        Assert.assertEquals(
                StringToTime.parseDateTime("morning", now).toDate(),
                StringToTime.parseDateTime("this morning", now).toDate());
    }

    @Test
    public void testNoon() {
        Date now = new Date();
        Assert.assertEquals(StringToTime.parseDateTime("12:00:00.000", now)
                .toDate(), StringToTime.parseDateTime("noon", now).toDate());
    }

    @Test
    public void testThisAfternoon() {
        Date now = new Date();
        Assert.assertEquals(StringToTime.parseDateTime("13:00:00.000", now)
                .toDate(), StringToTime.parseDateTime("this afternoon", now)
                .toDate());
        Assert.assertEquals(StringToTime.parseDateTime("afternoon", now)
                .toDate(), StringToTime.parseDateTime("this afternoon", now)
                .toDate());
    }

    @Test
    public void testThisEvening() {
        Date now = new Date();
        Assert.assertEquals(StringToTime.parseDateTime("17:00:00.000", now)
                .toDate(), StringToTime.parseDateTime("this evening", now)
                .toDate());
        Assert.assertEquals(
                StringToTime.parseDateTime("evening", now).toDate(),
                StringToTime.parseDateTime("this evening", now).toDate());
    }

    @Test
    public void testTonight() {
        Date now = new Date();
        Assert.assertEquals(StringToTime.parseDateTime("20:00:00.000", now)
                .toDate(), StringToTime.parseDateTime("tonight", now).toDate());
    }

    @Test
    @Ignore
    public void testIncrements() {
        Date now = new Date();

        Calendar cal = Calendar.getInstance();
        cal.setTime(now);
        cal.set(Calendar.HOUR_OF_DAY, cal.get(Calendar.HOUR_OF_DAY) + 1);
        Assert.assertEquals(cal.getTimeInMillis(),
                StringToTime.parseDateTime("+1 hour", now).toDate());

        cal.setTime(now);
        cal.set(Calendar.WEEK_OF_YEAR, cal.get(Calendar.WEEK_OF_YEAR) + 52);
        Assert.assertEquals(cal.getTimeInMillis(),
                StringToTime.parseDateTime("+52 weeks", now).toDate());

        Assert.assertEquals(StringToTime.parseDateTime("1 year", now).toDate(),
                StringToTime.parseDateTime("+1 year", now).toDate());

        Assert.assertEquals(
                StringToTime.parseDateTime("+1 year", now).toDate(),
                StringToTime.parseDateTime("+12 months", now).toDate());

        Assert.assertEquals(StringToTime.parseDateTime("+1 year 6 months", now)
                .toDate(), StringToTime.parseDateTime("+18 months", now)
                .toDate());

        Assert.assertEquals(
                StringToTime.parseDateTime("12 months 1 day 60 seconds", now)
                        .toDate(),
                StringToTime.parseDateTime("1 year 24 hours 1 minute", now)
                        .toDate());
    }

    @Test
    public void testDecrements() {
        Date now = new Date();

        Calendar cal = Calendar.getInstance();
        cal.setTime(now);
        cal.set(Calendar.HOUR_OF_DAY, cal.get(Calendar.HOUR_OF_DAY) - 1);
        Assert.assertEquals(new Date(cal.getTimeInMillis()), StringToTime
                .parseDateTime("-1 hour", now).toDate());
    }

    @Test
    public void testTomorrow() {
        Date now = new Date();
        Calendar cal = Calendar.getInstance();
        cal.setTime(now);
        cal.set(Calendar.DATE, cal.get(Calendar.DATE) + 1);
        Assert.assertEquals(new Date(cal.getTimeInMillis()), StringToTime
                .parseDateTime("tomorrow", now).toDate());
        Assert.assertEquals(StringToTime.parseDateTime("now +24 hours", now)
                .toDate(), StringToTime.parseDateTime("tomorrow", now).toDate());
    }

    @Test
    public void testTomorrowMorning() {
        Date now = new Date();
        Assert.assertEquals(
                StringToTime.parseDateTime("this morning +24 hours", now)
                        .toDate(),
                StringToTime.parseDateTime("tomorrow morning", now).toDate());
    }

    @Test
    public void testTomorrowNoon() {
        Date now = new Date();
        Assert.assertEquals(StringToTime.parseDateTime("noon +24 hours", now)
                .toDate(), StringToTime.parseDateTime("tomorrow noon", now)
                .toDate());
        Assert.assertEquals(StringToTime.parseDateTime("noon +24 hours", now)
                .toDate(), StringToTime.parseDateTime("noon tomorrow", now)
                .toDate());
    }

    @Test
    public void testTomorrowAfternoon() {
        Date now = new Date();
        Assert.assertEquals(
                StringToTime.parseDateTime("this afternoon +24 hours", now)
                        .toDate(),
                StringToTime.parseDateTime("tomorrow afternoon", now).toDate());
    }

    @Test
    public void testTomorrowEvening() {
        Date now = new Date();
        Assert.assertEquals(
                StringToTime.parseDateTime("this evening +24 hours", now)
                        .toDate(),
                StringToTime.parseDateTime("tomorrow evening", now).toDate());
    }

    @Test
    public void testTomorrowNight() {
        Date now = new Date();
        Assert.assertEquals(StringToTime
                .parseDateTime("tonight +24 hours", now).toDate(), StringToTime
                .parseDateTime("tomorrow night", now).toDate());
    }

    // e.g., October 26, 1981, or Oct 26, 1981, or 26 October 1981, or 26 Oct
    // 1981, or 26 Oct 81
    @Test
    @Ignore
    public void testLongHand() throws Exception {
        Assert.assertEquals(new SimpleDateFormat("M/d/y").parse("10/26/1981"),
                StringToTime.parseDateTime("October 26, 1981"));
        Assert.assertEquals(new SimpleDateFormat("M/d/y").parse("10/26/1981"),
                StringToTime.parseDateTime("Oct 26, 1981"));

        Assert.assertEquals(new SimpleDateFormat("M/d/y").parse("10/26/1981"),
                StringToTime.parseDateTime("26 October 1981"));
        Assert.assertEquals(new SimpleDateFormat("M/d/y").parse("10/26/1981"),
                StringToTime.parseDateTime("26 Oct 1981"));
        Assert.assertEquals(new SimpleDateFormat("M/d/y").parse("10/26/1981"),
                StringToTime.parseDateTime("26 Oct 81"));

        Assert.assertEquals(new SimpleDateFormat("M/d/y").parse("10/26/1981"),
                StringToTime.parseDateTime("26 october 1981"));
        Assert.assertEquals(new SimpleDateFormat("M/d/y").parse("10/26/1981"),
                StringToTime.parseDateTime("26 oct 1981"));
        Assert.assertEquals(new SimpleDateFormat("M/d/y").parse("10/26/1981"),
                StringToTime.parseDateTime("26 oct 81"));

        Assert.assertEquals(new SimpleDateFormat("M/d/y").parse("1/1/2000"),
                StringToTime.parseDateTime("1 Jan 2000"));
        Assert.assertEquals(new SimpleDateFormat("M/d/y").parse("1/1/2000"),
                StringToTime.parseDateTime("1 Jan 00"));

        Assert.assertEquals(new SimpleDateFormat("M/d/y").parse("1/1/2000"),
                StringToTime.parseDateTime("1 jan 2000"));
        Assert.assertEquals(new SimpleDateFormat("M/d/y").parse("1/1/2000"),
                StringToTime.parseDateTime("1 jan 00"));
    }

    // e.g., 10/26/1981 or 10/26/81
    @Test
    @Ignore
    public void testWithSlahesMonthFirst() throws Exception {
        Assert.assertEquals(new SimpleDateFormat("M/d/y").parse("10/26/1981"),
                StringToTime.parseDateTime("10/26/1981"));
        Assert.assertEquals(new SimpleDateFormat("M/d/y").parse("10/26/1981"),
                StringToTime.parseDateTime("10/26/81"));
    }

    // e.g., 1981/10/26
    @Test
    @Ignore
    public void testWithSlashesYearFirst() throws Exception {
        Assert.assertEquals(new SimpleDateFormat("M/d/y").parse("10/26/1981"),
                StringToTime.parseDateTime("1981/10/26"));
    }

    // e.g., October 26 and Oct 26
    @Test
    @Ignore
    public void testMonthAndDate() throws Exception {
        Date now = new Date();
        Calendar cal = Calendar.getInstance();
        cal.set(Calendar.MONTH, Calendar.OCTOBER);
        cal.set(Calendar.DATE, 26);
        Assert.assertEquals(new Date(cal.getTimeInMillis()), StringToTime
                .parseDateTime("October 26", now).toDate());
        Assert.assertEquals(StringToTime.parseDateTime("Oct 26", now).toDate(),
                StringToTime.parseDateTime("October 26", now).toDate());
    }

    // e.g., 10/26
    @Test
    @Ignore
    public void testWithSlahesMonthAndDate() throws Exception {
        Calendar cal = Calendar.getInstance();
        cal.set(Calendar.MONTH, Calendar.OCTOBER);
        cal.set(Calendar.DATE, 26);
        Assert.assertEquals(new Date(cal.getTimeInMillis()), StringToTime
                .parseDateTime("10/26").toDate());
    }

    // e.g., October or Oct
    @Test
    @Ignore
    public void testMonth() throws Exception {
        Date now = new Date();

        Assert.assertEquals(
                StringToTime.parseDateTime("October", now).toDate(),
                StringToTime.parseDateTime("Oct", now));

        Calendar cal = Calendar.getInstance();
        cal.setTime(now);

        Calendar cal2 = Calendar.getInstance();

        // it should be this year
        cal2.setTime(StringToTime.parseDateTime("January", now).toDate());
        Assert.assertEquals(cal.get(Calendar.YEAR), cal2.get(Calendar.YEAR));
        cal2.setTime(StringToTime.parseDateTime("December", now).toDate());
        Assert.assertEquals(cal.get(Calendar.YEAR), cal2.get(Calendar.YEAR));
    }

    @Test
    public void testDayOfWeek() throws Exception {
        Date now = new Date();
        Assert.assertEquals(StringToTime.parseDateTime("Friday", now).toDate(),
                StringToTime.parseDateTime("Fri", now).toDate());

        Calendar cal = Calendar.getInstance();
        cal.setTime(now);

        // if today's day of the week is greater than or equal to our test day
        // of the week (Wednesday)
        Calendar cal2 = Calendar.getInstance();
        if(cal.get(Calendar.DAY_OF_WEEK) >= 3) {// then the day of the week on
                                                // the date returned should be
                                                // next week
            cal2.setTime(StringToTime.parseDateTime("Wednesday", now).toDate());
            Assert.assertEquals(cal.get(Calendar.WEEK_OF_YEAR) + 1,
                    cal2.get(Calendar.WEEK_OF_YEAR));
        }
        else {
            // otherwise, it should be this year
            cal2.setTime(StringToTime.parseDateTime("Wednesday", now).toDate());
            Assert.assertEquals(cal.get(Calendar.WEEK_OF_YEAR),
                    cal2.get(Calendar.WEEK_OF_YEAR));
        }
    }

    @Test
    public void testNext() {
        Date now = new Date();
        Assert.assertEquals(StringToTime.parseDateTime("next January 15", now)
                .toDate(), StringToTime.parseDateTime("Jan 15", now).toDate());
        Assert.assertEquals(StringToTime.parseDateTime("next Dec", now)
                .toDate(), StringToTime.parseDateTime("December", now).toDate());
        Assert.assertEquals(StringToTime.parseDateTime("next Sunday", now)
                .toDate(), StringToTime.parseDateTime("Sun", now).toDate());
        Assert.assertEquals(StringToTime.parseDateTime("next Sat", now)
                .toDate(), StringToTime.parseDateTime("Saturday", now).toDate());
    }

    @Test
    public void testLast() {
        Date now = new Date();
        Assert.assertEquals(StringToTime.parseDateTime("last January 15", now)
                .toDate(), StringToTime.parseDateTime("Jan 15 -1 year", now)
                .toDate());
        Assert.assertEquals(StringToTime.parseDateTime("last Dec", now)
                .toDate(), StringToTime.parseDateTime("December -1 year", now)
                .toDate());
        Assert.assertEquals(StringToTime.parseDateTime("last Sunday", now)
                .toDate(), StringToTime.parseDateTime("Sun -1 week", now)
                .toDate());
        Assert.assertEquals(StringToTime.parseDateTime("last Sat", now)
                .toDate(), StringToTime.parseDateTime("Saturday -1 week", now)
                .toDate());
    }

}