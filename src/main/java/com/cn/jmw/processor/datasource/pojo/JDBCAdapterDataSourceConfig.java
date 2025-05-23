package com.cn.jmw.processor.datasource.pojo;

import com.cn.jmw.processor.datasource.enums.DataSourcePool;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author Jmwang
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class JdbcAdapterDataSourceConfig extends com.cn.jmw.processor.datasource.pojo.AbstractAdapterDataSourceConfig {

    private DataSourcePool dataSourcePool = DataSourcePool.HIKARICP;

    /**
     * 最大总连接数
     * maximumPoolSize 默认为10
     * <h1>参考<a href="https://github.com/brettwooldridge/HikariCP/wiki/About-Pool-Sizing">HikariCP配置最大总连接数</a><h1/>
     *
     * <h6>我们最近似乎在计算的其他领域了解到，少也是多。
     * 为什么只有 4 个线程的nginx Web 服务器的性能可以大大优于具有 100 个进程的Apache Web 服务器？如果你回想一下计算机科学 101，这难道不明显吗？<h1/>
     *
     * <h6/>即使只有单核 CPU 的计算机也能“同时”支持数十或数百个线程。但我们都知道，这仅仅是操作系统通过时间分片的魔力施展的花招。
     * 实际上，单核每次只能执行一个线程；然后操作系统切换上下文，该核执行另一个线程的代码，依此类推。
     * 计算的基本定律是，给定一个 CPU 资源，按顺序执行A和B总是比通过时间分片“同时”执行A和B更快。
     * 一旦线程数超过 CPU 核心数，添加更多线程只会让速度变慢，而不是更快。<h6/>
     *
     * <h1>连接数 = ((核心数 * 2) + 有效主轴数)</h1>
     */
    private int maximumPoolSize = 10;

    /**
     * 最小空闲连接数（推荐不设置，默认为 maximumPoolSize）
     * 与 maximumPoolSize 相同
     */
    private int minimumIdle;

    /**
     * 连接的默认自动提交行为，布尔值
     * autoCommit 默认为true
     */
    private boolean autoCommit = true;

    /**
     * 获取连接的最大等待时间（最小 250ms）
     * connectionTimeout 默认为30000
     */
    private int connectionTimeout = 30000;

    /**
     * 连接的最大空闲时间（最小 10000ms，0=永不移除）
     * idleTimeout 默认为600000
     */
    private int idleTimeout = 600000;

    /**
     * 保持连接活跃的频率（最小 30000ms，必须小于 maxLifetime）
     * keepaliveTime 默认为120000，不启用
     */
    private int keepaliveTime = 0;

    /**
     * 连接的最大生命周期（最小 30000ms，0=无限，受到 idleTimeout 限制）
     * maxLifetime 默认为1800000，无限
     */
    private int maxLifetime = 1800000;

    /**
     * 用于验证连接活性的查询（仅限旧驱动，不推荐使用 JDBC4）
     * validationQuery 默认为NULL
     */
    private String connectionTestQuery;

    /**
     * 用户定义的池名称，用于日志和 JMX（若未设置则自动生成）
     * poolName 默认为 自动生成
     */
    private String poolName;







    /**
     * 初始连接尝试的时间（0 = 立即启动池，<0 = 跳过，>0 = 阻塞，最小未指定）
     * initializationFailTimeout 默认为1
     */
    private int initializationFailTimeout = 1;

    /**
     * 将内部池查询隔离在自己的事务中（仅当 autoCommit 为 false 时）
     * isolateInternalQueries 默认为false
     */
    private boolean isolateInternalQueries = false;

    /**
     * 启用通过 JMX 暂停/恢复池以进行故障转移
     * allowPoolSuspension 默认为false
     */
    private boolean allowPoolSuspension = false;

    /**
     * 连接的默认只读模式（依赖数据库）
     * readOnly 默认为false
     */
    private boolean readOnly = false;

    /**
     * 	启用 JMX 管理 Bean 注册
     * 	registerMbeans 默认为false
     */
    private boolean registerMbeans = false;

    /**
     * 新建连接后执行的 SQL
     * connectionInitSql 默认为NULL
     */
    private String connectionInitSql = null;

    /**
     * 测试连接活性的最大时间（最小 250ms，必须小于 connectionTimeout）
     * validationTimeout 默认为5000
     */
    private int validationTimeout = 5000;

    /**
     * 记录可能连接泄漏的时间（0=禁用，最小 2000ms 以启用）
     * leakDetectionThreshold 默认为0
     */
    private int leakDetectionThreshold = 0;

    /**
     * 采样数量 （0=禁用，最小 1 以启用）
     * samplingCount 默认为0
     */
    private int samplingCount = 1000;
}
