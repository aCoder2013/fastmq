package com.song.fastmq.storage.common;

import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListMap;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author song
 */
public class MapTest {

    @Test
    public void testTreeMap() throws Exception {
        TreeMap<Long, Boolean> map = new TreeMap<>();
        map.put(1L, true);
        map.put(3L, true);
        map.put(5L, true);
        Assert.assertEquals(3L, map.ceilingKey(2L).longValue());
        Assert.assertEquals(5L, map.ceilingKey(4L).longValue());
    }

    @Test
    public void testConcurrentSkipListMap() throws Exception {
        NavigableMap<Long, Boolean> map = new ConcurrentSkipListMap<>();
        map.put(1L, true);
        map.put(3L, true);
        map.put(5L, true);
        Assert.assertEquals(3L, map.ceilingKey(2L).longValue());
        Assert.assertEquals(5L, map.ceilingKey(4L).longValue());
    }
}
