package org.elasticsearch.river.jdbc;

import org.junit.Test;

import java.util.Date;

import static junit.framework.Assert.assertEquals;

/**
 * Created with IntelliJ IDEA.
 * User: capitol
 * Date: 2012-12-09
 * Time: 07:46
 */
public class DateUtilTest {
    @Test
    public void testFormatDateISOLong() throws Exception {
        long input = 1000000000;

        String expResult = "1970-01-12T14:46:40Z";

        String result = DateUtil.formatDateISO(input);

        assertEquals("one million seconds", expResult, result);
    }

    @Test
    public void testFormatDateISODate() throws Exception {
        Date input = new Date(1000000000);

        String expResult = "1970-01-12T14:46:40Z";

        String result = DateUtil.formatDateISO(input);

        assertEquals("one million seconds", expResult, result);
    }
}
