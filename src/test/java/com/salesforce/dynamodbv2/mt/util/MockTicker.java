package com.salesforce.dynamodbv2.mt.util;

import com.google.common.base.Ticker;
import java.util.concurrent.TimeUnit;

/**
 * Ticker that allows test cases to control time.
 */
public class MockTicker extends Ticker {

    private long nanos;

    @Override
    public long read() {
        return nanos;
    }

    public void increment(long duration, TimeUnit unit) {
        nanos += unit.toNanos(duration);
    }

}
