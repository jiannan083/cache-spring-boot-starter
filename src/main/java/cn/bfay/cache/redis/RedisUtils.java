package cn.bfay.cache.redis;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.BoundHashOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.serializer.GenericJackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

/**
 * redis工具类.
 *
 * @author wangjiannan
 */
public class RedisUtils {
    private static final Logger logger = LoggerFactory.getLogger(RedisUtils.class);

    public RedisUtils() {
    }

    private static LettuceConnectionFactory factory;

    @Autowired
    public void setFactory(LettuceConnectionFactory connectionFactory) {
        RedisUtils.factory = connectionFactory;
    }

    private static StringRedisTemplate stringRedisTemplate;

    @Autowired
    public void setStringRedisTemplate(StringRedisTemplate stringRedisTemplate) {
        RedisUtils.stringRedisTemplate = stringRedisTemplate;
    }

    private static ObjectMapper mapper = new ObjectMapper();
    private static RedisTemplate<String, Object> template;

    @PostConstruct
    public static void init() {
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        //mapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
        mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        mapper.registerModule(new JavaTimeModule());
        mapper.setTimeZone(TimeZone.getTimeZone("GMT+8"));

        RedisTemplate<String, Object> template = new RedisTemplate<>();
        template.setConnectionFactory(RedisUtils.factory);
        GenericJackson2JsonRedisSerializer serializer = new GenericJackson2JsonRedisSerializer(mapper);
        template.setKeySerializer(new StringRedisSerializer());
        template.setHashKeySerializer(new StringRedisSerializer());
        template.setValueSerializer(serializer);
        template.setHashValueSerializer(serializer);
        template.afterPropertiesSet();
        RedisUtils.template = template;
    }

    /**
     * 设置String类型的值.
     *
     * @param key   键
     * @param value String类型的值
     */
    public static void setValue(String key, String value) {
        stringRedisTemplate.opsForValue().set(key, value);
    }

    /**
     * 设置String类型的值,带过期时间.
     *
     * @param key    键
     * @param value  String类型的值
     * @param expire 过期时间,单位:秒
     */
    public static void setValue(String key, String value, long expire) {
        stringRedisTemplate.opsForValue().set(key, value, expire, TimeUnit.SECONDS);
    }

    /**
     * 获取键为key的值.
     *
     * @param key 键
     * @return 返回String
     */
    public static String getValue(String key) {
        return stringRedisTemplate.opsForValue().get(key);
    }

    /**
     * 设置键为key的值.
     *
     * @param key   键
     * @param value 值
     */
    public static void setValue(String key, Object value) {
        template.opsForValue().set(key, value);
    }

    /**
     * 设置值,带过期时间.
     *
     * @param key    键
     * @param value  值
     * @param expire 有效时间,单位:秒
     */
    public static void setValue(String key, Object value, long expire) {
        template.opsForValue().set(key, value, expire, TimeUnit.SECONDS);
    }

    /**
     * 获取键为key的值.
     *
     * @param key   键
     * @param clazz 需要转换成的类型
     * @param <T>   类型
     * @return 返回转换后的类型
     */
    public static <T> T getValue(String key, Class<T> clazz) {
        String originValue = getValue(key);
        if (originValue == null) {
            return null;
        }
        try {
            return mapper.readValue(originValue, clazz);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            return null;
        }
    }

    /**
     * 获取键为key的值.
     *
     * @param key  键
     * @param type 需要转换成的类型
     * @param <T>  类型
     * @return 返回转换后的类型
     */
    public static <T> T getValue(String key, TypeReference type) {
        String originValue = getValue(key);
        if (originValue == null) {
            return null;
        }
        try {
            return mapper.readValue(originValue, type);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            return null;
        }
    }

    /**
     * 增加。初始0，步长1.
     *
     * @param key 键
     * @return long
     */
    public static Long increment(String key) {
        return template.opsForValue().increment(key);
    }

    /**
     * 增加。初始0，步长delta.
     *
     * @param key   键
     * @param delta 步长
     * @return long
     */
    public static Long increment(String key, long delta) {
        return template.opsForValue().increment(key, delta);
    }

    /**
     * 增加。初始0，步长delta.
     *
     * @param key   键
     * @param delta 步长
     * @return long
     */
    public static Double increment(String key, double delta) {
        return template.opsForValue().increment(key, delta);
    }

    /**
     * 减少。初始0，步长1.
     *
     * @param key 键
     * @return long
     */
    public static Long decrement(String key) {
        return template.opsForValue().decrement(key);
    }

    /**
     * 减少。初始0，步长delta.
     *
     * @param key   键
     * @param delta 步长
     * @return long
     */
    public static Long decrement(String key, long delta) {
        return template.opsForValue().decrement(key, delta);
    }

    // ----- map start ------

    /**
     * 向键为key的hashmap中添加值.
     *
     * @param key   键
     * @param field 字段
     * @param value 值
     */
    public static void mapPutValue(String key, String field, Object value) {
        template.boundHashOps(key).put(field, value);
    }

    /**
     * 向键为key的hashmap中添加值.
     *
     * @param key    键
     * @param field  字段
     * @param value  值
     * @param expire 有效时间,单位:秒
     */
    public static void mapPutValue(String key, String field, Object value, long expire) {
        BoundHashOperations<String, Object, Object> ops = template.boundHashOps(key);
        ops.put(field, value);
        ops.expire(expire, TimeUnit.SECONDS);
    }

    /**
     * 向键为key的hashmap中添加值.
     *
     * @param key   键
     * @param field 字段
     * @param value 值
     * @param date  有效时间
     */
    public static void mapPutValue(String key, String field, Object value, Date date) {
        BoundHashOperations<String, Object, Object> ops = template.boundHashOps(key);
        ops.put(field, value);
        ops.expireAt(date);
    }

    /**
     * 设置键值为key的map.
     *
     * @param key 键
     * @param map map对象实例
     */
    public static void mapPutMap(String key, Map<Object, Object> map) {
        template.boundHashOps(key).putAll(map);
    }

    /**
     * 设置键值为key的map。有过期时间.
     *
     * @param key    键
     * @param map    map
     * @param expire 过期时间,单位:秒
     */
    public static void mapPutMap(String key, Map<Object, Object> map, long expire) {
        BoundHashOperations<String, Object, Object> ops = template.boundHashOps(key);
        ops.putAll(map);
        ops.expire(expire, TimeUnit.SECONDS);
    }

    /**
     * 设置键值为key的map。有过期时间.
     *
     * @param key  键
     * @param map  map
     * @param date 有效时间
     */
    public static void mapPutMap(String key, Map<Object, Object> map, Date date) {
        BoundHashOperations<String, Object, Object> ops = template.boundHashOps(key);
        ops.putAll(map);
        ops.expireAt(date);
    }

    /**
     * 获取键为key的map.
     *
     * @param key 键
     * @return 返回map
     */
    public static Map<Object, Object> mapGetMap(String key) {
        return template.opsForHash().entries(key);
    }

    /**
     * 获取键为key的map.
     *
     * @param key   键
     * @param clazz 需要转换成的类型
     * @param <T>   类型
     * @return 返回map
     */
    public static <T> Map<String, T> mapGetMap(String key, Class<T> clazz) {

        Map<String, T> resultMap = new HashMap<>();
        mapGetMap(key).forEach((key1, value) -> {
            try {
                resultMap.put((String) key1, mapper.readValue(mapper.writeValueAsString(value), clazz));
            } catch (IOException e) {
                logger.error(e.getMessage(), e);
            }
        });
        return resultMap;
    }

    /**
     * 获取键为key的map.
     *
     * @param key  键
     * @param type 需要转换成的类型
     * @param <T>  类型
     * @return 返回map
     */
    public static <T> Map<String, T> mapGetMap(String key, TypeReference type) {

        Map<String, T> resultMap = new HashMap<>();
        mapGetMap(key).forEach((key1, value) -> {
            try {
                resultMap.put((String) key1, mapper.readValue(mapper.writeValueAsString(value), type));
            } catch (IOException e) {
                logger.error(e.getMessage(), e);
            }
        });
        return resultMap;
    }

    /**
     * 获取键为key的map中的指定字段名的值.
     *
     * @param key   键
     * @param field 字段名称
     * @return 返回值
     */
    public static Object mapGetValue(String key, String field) {
        return template.opsForHash().get(key, field);
    }

    /**
     * 获取键为key的map中的指定字段名的值.
     *
     * @param key   键
     * @param field 字段
     * @param clazz 需要转换成的类型
     * @param <T>   类型
     * @return 返回转换后的类型
     */
    public static <T> T mapGetValue(String key, String field, Class<T> clazz) {
        Object originValue = mapGetValue(key, field);
        if (originValue == null) {
            return null;
        }
        try {
            return mapper.readValue(String.valueOf(originValue), clazz);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            return null;
        }
    }

    /**
     * 获取键为key的map中的指定字段名的值.
     *
     * @param key   键
     * @param field 字段
     * @param type  需要转换成的类型
     * @param <T>   类型
     * @return 返回转换后的类型
     */
    public static <T> T mapGetValue(String key, String field, TypeReference type) {
        Object originValue = mapGetValue(key, field);
        if (originValue == null) {
            return null;
        }
        try {
            return mapper.readValue(String.valueOf(originValue), type);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            return null;
        }
    }

    /**
     * 删除哈希表子键值.
     *
     * @param key   键
     * @param field 字段名称
     */
    public static void mapDeleteValue(String key, String field) {
        if (template.opsForHash().hasKey(key, field)) {
            template.opsForHash().delete(key, field);
        }
    }
    // ----- map end ------

    // ----- list start ------

    /**
     * 将list放入缓存.
     *
     * @param key   键
     * @param value 值
     * @return long
     */
    public static Long listRightPush(String key, Object value) {
        return template.opsForList().rightPush(key, value);
    }

    /**
     * 将list放入缓存.
     *
     * @param key    键
     * @param value  值
     * @param expire 时间(秒)
     * @return long
     */
    public static Long listRightPush(String key, Object value, long expire) {
        return template.opsForList().rightPush(key, value, expire);
    }

    /**
     * 将list放入缓存.
     *
     * @param key   键
     * @param value 值
     * @return long
     */
    public static Long listRightPushAll(String key, List<Object> value) {
        return template.opsForList().rightPushAll(key, value);
    }

    /**
     * 将list放入缓存.
     *
     * @param key    键
     * @param value  值
     * @param expire 时间(秒)
     * @return long
     */
    public static Long listRightPushAll(String key, List<Object> value, long expire) {
        return template.opsForList().rightPushAll(key, value, expire);
    }

    /**
     * 获取list缓存的长度.
     *
     * @param key 键
     * @return long
     */
    public static Long listGetSize(String key) {
        return template.opsForList().size(key);
    }

    /**
     * 获取list缓存的内容.
     *
     * @param key   键
     * @param start 开始
     * @param end   结束  0 到 -1代表所有值
     * @return list
     */
    public static List<Object> listGetRange(String key, long start, long end) {
        return template.opsForList().range(key, start, end);
    }

    /**
     * 通过索引 获取list中的值.
     *
     * @param key   键
     * @param index 索引  index>=0时， 0 表头，1 第二个元素，依次类推；index<0时，-1，表尾，-2倒数第二个元素，依次类推
     * @return Object
     */
    public static Object listGetIndex(String key, long index) {
        return template.opsForList().index(key, index);
    }

    /**
     * 根据索引修改list中的某条数据.
     *
     * @param key   键
     * @param index 索引
     * @param value 值
     */
    public static void listUpdateIndex(String key, long index, Object value) {
        template.opsForList().set(key, index, value);
    }

    /**
     * 移除N个值为value.
     *
     * @param key   键
     * @param count 移除多少个
     * @param value 值
     * @return 移除的个数
     */
    public static Long listRemove(String key, long count, Object value) {
        return template.opsForList().remove(key, count, value);
    }

    // ----- list end ------
    // ----- set start ------

    /**
     * 将数据放入set缓存.
     *
     * @param key    键
     * @param values 值 可以是多个
     * @return 成功个数
     */
    public static Long setAdd(String key, Object... values) {
        return template.opsForSet().add(key, values);
    }

    /**
     * 将set数据放入缓存.
     *
     * @param key    键
     * @param expire 时间(秒)
     * @param values 值 可以是多个
     * @return 成功个数
     */
    public static Long setAdd(String key, long expire, Object... values) {
        Long count = template.opsForSet().add(key, values);
        template.expire(key, expire, TimeUnit.SECONDS);
        return count;
    }

    /**
     * 移除值为value的.
     *
     * @param key    键
     * @param values 值 可以是多个
     * @return 移除的个数
     */
    public static Long setRemove(String key, Object... values) {
        return template.opsForSet().remove(key, values);
    }

    /**
     * 获取set缓存的长度.
     *
     * @param key 键
     * @return long
     */
    public static Long setGetSize(String key) {
        return template.opsForSet().size(key);
    }

    /**
     * 根据key获取Set中的所有值.
     *
     * @param key 键
     * @return set
     */
    public static Set<Object> setGetMember(String key) {
        return template.opsForSet().members(key);
    }

    /**
     * 根据value从一个set中查询,是否存在.
     *
     * @param key   键
     * @param value 值
     * @return true 存在 false不存在
     */
    public static Boolean setHasMember(String key, Object value) {
        return template.opsForSet().isMember(key, value);
    }
    // ----- set end ------

    /**
     * setNX.
     *
     * @param key   键
     * @param value 值
     * @return boolean
     */
    public static Boolean setNx(String key, Object value) {
        return template.opsForValue().setIfAbsent(key, value);
    }

    /**
     * setNX.
     *
     * @param key    键
     * @param value  值
     * @param expire 过期时间，秒
     * @return boolean
     */
    public static Boolean setNx(String key, Object value, long expire) {
        return template.opsForValue().setIfAbsent(key, value, expire, TimeUnit.SECONDS);
    }

    /**
     * 返回旧值，设置新值.
     *
     * @param key   键
     * @param value 值
     * @return string
     */
    public static String getSet(String key, String value) {
        return stringRedisTemplate.opsForValue().getAndSet(key, value);
    }


    //public static Boolean setNx(String key, Object value, Duration timeout) {
    //    return template.opsForValue().setIfAbsent(key, value, timeout);
    //}

    // ----- common start ------

    /**
     * 查询redis里是否有对应的key.
     *
     * @param key 要查询的key
     * @return true:有, false:无
     */
    public static Boolean hasKey(String key) {
        return template.hasKey(key);
    }

    /**
     * 删除键值.
     *
     * @param key 键
     */
    public static void delete(String key) {
        template.delete(key);
    }

    /**
     * 延长缓存时间.
     *
     * @param key     键值
     * @param timeout 时间长度(秒)
     * @return 操作结果
     */
    public static Boolean expire(String key, long timeout) {
        return template.expire(key, timeout, TimeUnit.SECONDS);
    }

    /**
     * 延长缓存时间.
     *
     * @param key     键值
     * @param timeout 时间长度
     * @param unit    时间单位
     * @return 操作结果
     */
    public static Boolean expire(String key, long timeout, TimeUnit unit) {
        return template.expire(key, timeout, unit);
    }

    /**
     * 缓存剩余时间.
     * -1, 如果key没有到期超时
     * -2, 如果键不存在
     *
     * @param key 键值
     * @return 秒
     */
    public static Long getExpireTime(String key) {
        return template.getExpire(key, TimeUnit.SECONDS);
    }
    // ----- common end ------

    /**
     * 根据给定的布隆过滤器添加值.
     */
    public <T> void addByBloomFilter(BloomFilterHelper<T> bloomFilterHelper, String key, T value) {
        Preconditions.checkArgument(bloomFilterHelper != null, "bloomFilterHelper不能为空");
        int[] offset = bloomFilterHelper.murmurHashOffset(value);
        for (int i : offset) {
            template.opsForValue().setBit(key, i, true);
        }
    }

    /**
     * 根据给定的布隆过滤器判断值是否存在.
     */
    public <T> boolean includeByBloomFilter(BloomFilterHelper<T> bloomFilterHelper, String key, T value) {
        Preconditions.checkArgument(bloomFilterHelper != null, "bloomFilterHelper不能为空");
        int[] offset = bloomFilterHelper.murmurHashOffset(value);
        boolean result = true;
        for (int i : offset) {
            Boolean bl = template.opsForValue().getBit(key, i);
            if (bl == null || !bl) {
                result = false;
                break;
            }
        }
        return result;
    }
    //public <T> boolean includeByBloomFilter(BloomFilterHelper<T> bloomFilterHelper, String key, T value) {
    //    Preconditions.checkArgument(bloomFilterHelper != null, "bloomFilterHelper不能为空");
    //    int[] offset = bloomFilterHelper.murmurHashOffset(value);
    //    for (int i : offset) {
    //        if (!template.opsForValue().getBit(key, i)) {
    //            return false;
    //        }
    //    }
    //
    //    return true;
    //}

    //-------------------------------
    ///**
    // * 获取key值列表.
    // *
    // * @param prefix key前缀
    // * @return {@link List}
    // */
    //public static List<String> keys(String prefix) {
    //    Set<String> result = stringRedisTemplate.keys(prefix + "*");
    //    return result == null ? null : new ArrayList<>(result);
    //}

    ///**
    // * 批量获取值.
    // *
    // * @param keys 键列表
    // * @return 值列表
    // */
    //public static List<String> multiGet(List<String> keys) {
    //    return stringRedisTemplate.opsForValue().multiGet(keys);
    //}

    ///**
    // * int value.
    // */
    //public static Integer getInteger(String key) {
    //    return (Integer) template.opsForValue().get(key);
    //}
    //
    //public static void setInteger(String key, Integer value, long expire) {
    //    template.opsForValue().set(key, value, expire, TimeUnit.SECONDS);
    //    //BoundValueOperations<String, Object> ops = template.boundValueOps(key);
    //    //ops.set(value);
    //    //ops.expire(expire, TimeUnit.SECONDS);
    //}
    //
    //public static void setInteger(String key, Integer value) {
    //    template.opsForValue().set(key, value);
    //    //template.boundValueOps(key).set(value);
    //}
}
