package com.cn.jmw.pojo;

import java.util.Objects;

/**
 * Context类用于表示处理上下文。
 * <p>
 * 此类封装了输入和输出的泛型，提供了构造函数以便于创建上下文实例。
 * </p>
 *
 * @author Jmwang
 * @param <T> 输入类型
 * @param <R> 输出类型
 */
public class Context<T, R> {
    /**
     * 输入数据，可能为 null。
     */
    private T input;

    /**
     * 输出数据，可能为 null。
     */
    private R output;

    /**
     * 默认构造函数，用于创建一个空的Context实例。
     */
    public Context() {}

    /**
     * 带输入参数的构造函数，用于初始化输入数据。
     *
     * @param input 输入数据
     */
    public Context(T input) {
        this.input = input;
    }

    /**
     * 带输入和输出参数的构造函数，用于完整初始化上下文。
     *
     * @param input 输入数据
     * @param output 输出数据
     */
    public Context(T input, R output) {
        this.input = input;
        this.output = output;
    }

    /**
     * 获取输入参数。
     *
     * @return 输入参数
     */
    public T getInput() {
        return input;
    }

    /**
     * 获取输出参数。
     *
     * @return 输出参数
     */
    public R getOutput() {
        return output;
    }

    /**
     * 设置输入参数。
     *
     * @param input 输入参数
     */
    public void setInput(T input) {
        this.input = input;
    }

    /**
     * 设置输出参数。
     *
     * @param output 输出参数
     */
    public void setOutput(R output) {
        this.output = output;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Context<?, ?> context = (Context<?, ?>) o;
        return Objects.equals(input, context.input) && Objects.equals(output, context.output);
    }

    @Override
    public int hashCode() {
        return Objects.hash(input, output);
    }

    @Override
    public String toString() {
        return "Context{" +
                "input=" + input +
                ", output=" + output +
                '}';
    }
}