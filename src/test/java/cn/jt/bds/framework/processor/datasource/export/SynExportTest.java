package cn.jt.bds.framework.processor.datasource.export;

import cn.jt.bds.framework.processor.datasource.enums.DatabaseEnum;
import cn.jt.bds.framework.processor.datasource.enums.FileTypeEnum;
import cn.jt.bds.framework.processor.datasource.factory.DatabaseAdapterFactory;
import cn.jt.bds.framework.processor.datasource.jdbc.adapter.DorisJDBCAdapter;
import cn.jt.bds.framework.processor.datasource.jdbc.adapter.pojo.IntoOutFile;
import cn.jt.bds.framework.processor.datasource.pojo.JDBCConnectionEntity;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class SynExportTest {

    private DorisJDBCAdapter adapter;
    private String label;

    @Before
    public void setUp() throws Exception {
        JDBCConnectionEntity jdbcConnectionEntity = new JDBCConnectionEntity(DatabaseEnum.DORIS, "192.168.10.202", 9030, "", "root", "123456aA!@");
        adapter = DatabaseAdapterFactory.getAdapter(jdbcConnectionEntity, DorisJDBCAdapter.class);
        label = "1008615";//UUID.randomUUID().toString();
    }

    @Test
    public void synExport() {
        IntoOutFile intoOutFile = adapter.synExport("SELECT * FROM bds_log.bds_rds_log limit 5000000", "/data/export/","2GB", FileTypeEnum.CSV_WITH_NAMES);
        System.out.println(intoOutFile);
    }
}