package com.cn.jmw.pojo;

import java.time.LocalDateTime;

/**
 * 样本结果
 *
 * @author Jmwang
 */
import java.time.LocalDateTime;
import java.util.Objects;

/**
 * SampleResult类用于表示抽样操作的结果。
 * <p>
 * 该类包含抽样方式、抽样数量和抽样时间等信息，用于记录抽样过程的关键数据。
 * 适用于需要记录和分析抽样结果的场景。
 * </p>
 *
 * @author Jmwang
 */
public class SampleResult {

    /**
     * 抽样方式，例如 "随机抽样" 或 "分层抽样"，可能为 null。
     */
    private String sampleMethod;

    /**
     * 抽样数量，非负整数。
     */
    private long sampleCount;

    /**
     * 抽样时间，可能为 null。
     */
    private LocalDateTime sampleTime;

    /**
     * 默认构造函数，用于创建一个空的SampleResult实例。
     */
    public SampleResult() {}

    /**
     * 带参数的构造函数，用于初始化抽样方式、抽样数量和抽样时间。
     *
     * @param sampleMethod 抽样方式，可能为 null。
     * @param sampleCount  抽样数量，非负整数。
     * @param sampleTime   抽样时间，可能为 null。
     */
    public SampleResult(String sampleMethod, long sampleCount, LocalDateTime sampleTime) {
        this.sampleMethod = sampleMethod;
        this.sampleCount = sampleCount;
        this.sampleTime = sampleTime;
    }

    /**
     * 获取抽样方式。
     *
     * @return 抽样方式，返回当前对象的 sampleMethod 字段值。
     */
    public String getSampleMethod() {
        return sampleMethod;
    }

    /**
     * 设置抽样方式。
     *
     * @param sampleMethod 抽样方式，用于更新当前对象的 sampleMethod 字段值。
     */
    public void setSampleMethod(String sampleMethod) {
        this.sampleMethod = sampleMethod;
    }

    /**
     * 获取抽样数量。
     *
     * @return 抽样数量，返回当前对象的 sampleCount 字段值。
     */
    public long getSampleCount() {
        return sampleCount;
    }

    /**
     * 设置抽样数量。
     *
     * @param sampleCount 抽样数量，用于更新当前对象的 sampleCount 字段值。
     */
    public void setSampleCount(long sampleCount) {
        this.sampleCount = sampleCount;
    }

    /**
     * 获取抽样时间。
     *
     * @return 抽样时间，返回当前对象的 sampleTime 字段值。
     */
    public LocalDateTime getSampleTime() {
        return sampleTime;
    }

    /**
     * 设置抽样时间。
     *
     * @param sampleTime 抽样时间，用于更新当前对象的 sampleTime 字段值。
     */
    public void setSampleTime(LocalDateTime sampleTime) {
        this.sampleTime = sampleTime;
    }

    @Override
    public String toString() {
        return "SampleResult{" +
                "sampleMethod='" + sampleMethod + '\'' +
                ", sampleCount=" + sampleCount +
                ", sampleTime=" + sampleTime +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;

        SampleResult that = (SampleResult) o;
        return sampleCount == that.sampleCount && Objects.equals(sampleMethod, that.sampleMethod) && Objects.equals(sampleTime, that.sampleTime);
    }

    @Override
    public int hashCode() {
        int result = Objects.hashCode(sampleMethod);
        result = 31 * result + Long.hashCode(sampleCount);
        result = 31 * result + Objects.hashCode(sampleTime);
        return result;
    }
}
