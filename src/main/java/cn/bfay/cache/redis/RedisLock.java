package cn.bfay.cache.redis;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

/**
 * RedisLock.
 *
 * @author wangjiannan
 * @since 2020/1/7
 */
public class RedisLock {
    private static Logger log = LoggerFactory.getLogger(RedisLock.class);

    // 毫秒
    private static final long TRY_INTERVAL_MILLIS = 100;
    private static final String LOCK_ROOT_KEY = "LOCK";
    private static final String LOCK_VALUE = "LOCK_VALUE";

    private static volatile boolean locked = false;

    /**
     * 锁.
     *
     * @param key    键
     * @param expire 过期时间，秒
     * @return boolean
     */
    public static boolean lock(String key, long expire) {
        long tryMaxCount = expire * 1000 / TRY_INTERVAL_MILLIS + 1;
        return lock(key, LOCK_VALUE, expire, TRY_INTERVAL_MILLIS, tryMaxCount);
    }

    /**
     * 锁.
     *
     * @param key         键
     * @param expire      过期时间，秒
     * @param tryMaxCount 最大的轮询次数
     * @return boolean
     */
    public static boolean lock(String key, long expire, long tryMaxCount) {
        return lock(key, LOCK_VALUE, expire, TRY_INTERVAL_MILLIS, tryMaxCount);
    }

    /**
     * 锁.
     *
     * @param key               键
     * @param value             值
     * @param expire            过期时间，秒
     * @param tryIntervalMillis 轮询的时间间隔(毫秒)
     * @param tryMaxCount       最大的轮询次数
     * @return boolean
     */
    public static boolean lock(String key, String value, long expire, long tryIntervalMillis, long tryMaxCount) {
        int tryCount = 0;
        while (tryCount++ <= tryMaxCount) {
            try {
                if (RedisUtils.setNx(generateLockKey(key), value, expire)) {
                    log.debug(Thread.currentThread().getName() + "获取到了锁");
                    locked = true;
                    return true;
                }

                Thread.sleep(tryIntervalMillis + new Random().nextInt(100));
                log.debug(Thread.currentThread().getName() + "未获取到了锁，重试" + tryCount);
            } catch (Exception e) {
                log.error("获取锁异常", e);
                return false;
            }
        }
        log.debug(Thread.currentThread().getName() + "未获取到了锁，重试结束");
        return false;
    }

    /**
     * Acqurired lock release.
     */
    public static void unlock(String lockKey) {
        if (locked) {
            log.debug(Thread.currentThread().getName() + "解锁操作");
            locked = false;
            RedisUtils.delete(generateLockKey(lockKey));
        }
    }

    private static String generateLockKey(String lockKey) {
        return String.format("%s%s%s", LOCK_ROOT_KEY, ":", lockKey);
    }
}
