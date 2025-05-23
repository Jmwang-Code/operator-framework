package com.cn.jmw.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * NotRecommended注解用于标记不推荐使用的方法、类或字段。
 * <p>
 * 该注解适用于底层内部功能或不建议直接调用的元素，建议查看替代方案。
 * </p>
 *
 * @author Jmwang
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD, ElementType.TYPE, ElementType.FIELD})
public @interface NotRecommended {
    /**
     * 不推荐使用的原因。
     *
     * @return 返回不推荐使用的原因，默认提供通用提示。
     */
    String reason() default "此方法为内部实现，不建议直接调用，可能在未来版本中移除。";
}