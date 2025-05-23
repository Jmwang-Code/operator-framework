package com.cn.jmw.processor;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

import static com.cn.jmw.common.exception.enums.StructuredErrorCodeConstants.MUST_PROVIDE_TYPE_PARAMETER;
import static com.cn.jmw.common.exception.util.ServiceExceptionUtil.exception;

/**
 * 基本处理器抽象类，实现了Processor接口。该类提供了关于输入数据类型的处理能力。
 *
 * @author Jmwang
 * @param <T> 输入数据类型
 * @param <R> 输出数据类型
 */
public abstract class BaseProcessor<T, R> implements Processor<T, R> {

    /**
     * 输入数据的类型
     */
    private final Type inputType;

    /**
     * 可变参数字段，用于存储额外数据
     */
    private Object[] data;

    /**
     * 默认构造函数，自动获取输入数据的类型。
     */
    public BaseProcessor() {
        try {
            ParameterizedType genericSuperclass = (ParameterizedType) getClass().getGenericSuperclass();
            //获取类型参数
            this.inputType = genericSuperclass.getActualTypeArguments()[0];
        } catch (ClassCastException e) {
            //抛出类型参数未提供的异常
            throw exception(MUST_PROVIDE_TYPE_PARAMETER);
        }
    }

    /**
     * 带参数的构造函数，可指定输入数据的类型。
     *
     * @param inputType 指定的输入数据类型
     */
    public BaseProcessor(Type inputType) {
        this.inputType = inputType;
    }

    /**
     * 获取输入数据的类型。
     *
     * @return 输入数据的类型
     */
    @Override
    public Type getInputType() {
        return inputType;
    }

    /**
     * 设置额外数据。
     *
     * @param data 可变参数，额外的数据
     */
    @Override
    public void setData(Object... data) {
        this.data = data;
    }

    /**
     * 获取额外数据。
     *
     * @return 额外数据的数组
     */
    @Override
    public Object[] getData() {
        return data;
    }

    /**
     * 处理输入数据并返回处理结果，具体实现由子类定义。
     *
     * @param input 输入数据
     * @param data 可变参数，额外的数据
     * @return 处理结果
     * @throws Exception 处理过程中可能抛出的异常
     */
    @Override
    public abstract R process(T input, Object... data) throws Exception;  // 抽象方法，由子类实现
}