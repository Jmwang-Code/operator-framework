package cn.jt.bds.framework.processor.datasource.export;

import cn.jt.bds.framework.processor.datasource.enums.DatabaseEnum;
import cn.jt.bds.framework.processor.datasource.factory.DatabaseAdapterFactory;
import cn.jt.bds.framework.processor.datasource.jdbc.adapter.DorisJDBCAdapter;
import cn.jt.bds.framework.processor.datasource.jdbc.adapter.pojo.ShowExport;
import cn.jt.bds.framework.processor.datasource.pojo.JDBCConnectionEntity;
import org.junit.Before;
import org.junit.Test;

import java.util.List;


public class ASynExportTest {

    private DorisJDBCAdapter adapter;
    private String label;

    @Before
    public void setUp() throws Exception {
        JDBCConnectionEntity jdbcConnectionEntity = new JDBCConnectionEntity(DatabaseEnum.DORIS, "192.168.10.202", 9030, "", "root", "123456aA!@");
        adapter = DatabaseAdapterFactory.getAdapter(jdbcConnectionEntity, DorisJDBCAdapter.class);
        label = "1008622";//UUID.randomUUID().toString();
    }


    @Test
    public void aSynExport() throws IllegalAccessException, InstantiationException {
        adapter.aSynExport("bds_log", "bds_asset_info","","","", label);
    }

    @Test
    public void queryASynExportJobStatus() {
        ShowExport showExport = adapter.queryASynExportJobStatus("bds_log", label);
        System.out.println(showExport);
    }

    @Test
    public void testQueryASynExportJobStatus() {
        List<ShowExport> showExports = adapter.queryASynExportJobStatus("bds_log");
        System.out.println(showExports);
    }

    @Test
    public void stopASynExportJob() {
        adapter.stopASynExportJob(label);
    }
}