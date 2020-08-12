package cn.bfay.cache.redis;

/**
 * RedisKeyBuilder.
 *
 * @author wangjiannan
 */
public class RedisKeyBuilder {
    /**
     * 生成redis key.
     *
     * @param format  格式化字符串
     * @param objects 参数列表
     * @return string
     */
    public static String generateRedisKey(String format, Object... objects) {
        return String.format(format, objects);
    }
}
