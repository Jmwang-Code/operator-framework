package cn.jt.bds.framework.processor.datasource.export;

import cn.jt.bds.framework.processor.datasource.enums.DatabaseEnum;
import cn.jt.bds.framework.processor.datasource.enums.FileTypeEnum;
import cn.jt.bds.framework.processor.datasource.factory.DatabaseAdapterFactory;
import cn.jt.bds.framework.processor.datasource.jdbc.adapter.DorisJDBCAdapter;
import cn.jt.bds.framework.processor.datasource.jdbc.adapter.pojo.IntoOutFile;
import cn.jt.bds.framework.processor.datasource.pojo.JDBCConnectionEntity;
import lombok.SneakyThrows;
import org.apache.commons.dbutils.ResultSetHandler;
import org.junit.Before;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;

public class TestExcel {

    private DorisJDBCAdapter adapter;
    private String label;

    @Before
    public void setUp() throws Exception {
        JDBCConnectionEntity jdbcConnectionEntity = new JDBCConnectionEntity(DatabaseEnum.DORIS, "192.168.10.202", 9030, "", "root", "123456aA!@");
        adapter = DatabaseAdapterFactory.getAdapter(jdbcConnectionEntity, DorisJDBCAdapter.class);
        label = "1008622";//UUID.randomUUID().toString();
    }

    @Test
    public void 同步() {
        long l = System.currentTimeMillis();
        IntoOutFile intoOutFile = adapter.synExport("SELECT * FROM bds_log.bds_rds_log limit 5000000", "/tmp/bds_log","2GB", FileTypeEnum.CSV_WITH_NAMES);
        System.out.println(intoOutFile);
        System.out.println("同步耗时:" + ((System.currentTimeMillis() - l) / 1000) + "秒");
        //10w - 30w 一秒
    }

// To limit the memory usage while processing the ResultSet in batches of 5000 records and clearing the memory after each batch, you can modify the code as follows:

    @Test
    public void 流处理() {
        //并不是适合太大数据量的流处理，因为会消耗大量内存
        long l = System.currentTimeMillis();
        AtomicInteger atomicInteger = new AtomicInteger(0);
        int batchSize = 100000;

        // Query in batches of 5000 records
        try {
            adapter.queryStream("SELECT * FROM bds_log.bds_rds_log LIMIT 10000000", new ResultSetHandler<Void>() {
                @SneakyThrows
                @Override
                public Void handle(ResultSet resultSet) throws SQLException {
                    while (resultSet.next()) {
                        if (atomicInteger.get() % batchSize == 0) {
                            System.out.println(atomicInteger.get());
                            Thread.sleep(1000);
                        }
                        atomicInteger.incrementAndGet();
                        resultSet.getString("event_time");
                        resultSet.getString("event_owner");
                        resultSet.getString("device_type");
                        resultSet.getString("event_id");
                        resultSet.getString("device_ip");
                        resultSet.getString("_time");
                        resultSet.getString("_year");
                        resultSet.getString("_month");
                        resultSet.getString("_day");
                        resultSet.getString("_hour");
                        resultSet.getString("_minute");
                        resultSet.getString("_second");
                        resultSet.getString("_weekday");
                        resultSet.getString("event_raw");
                        resultSet.getString("source_type");
                        resultSet.getString("src_ip");
                        resultSet.getString("user_name");
                        resultSet.getString("db_type");
                        resultSet.getString("db_name");
                        resultSet.getString("db_table");
                        resultSet.getString("db_column");
                    }
                    return null;
                }
            });
        } catch (SQLException e) {
            System.out.println("同步耗时:" + ((System.currentTimeMillis() - l) / 1000) + "秒");
            throw new RuntimeException(e);
        }

        System.out.println(atomicInteger.get());
        System.out.println("同步耗时:" + ((System.currentTimeMillis() - l) / 1000) + "秒");
    }
}
