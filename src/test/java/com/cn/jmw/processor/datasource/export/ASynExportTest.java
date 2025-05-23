package com.cn.jmw.processor.datasource.export;

import com.cn.jmw.processor.datasource.enums.DatabaseEnum;
import com.cn.jmw.processor.datasource.factory.DatabaseAdapterFactory;
import com.cn.jmw.processor.datasource.jdbc.adapter.DorisJdbcAdapter;
import com.cn.jmw.processor.datasource.pojo.JdbcConnectionEntity;
import com.cn.jmw.processor.datasource.pojo.ShowExport;
import org.junit.Before;
import org.junit.Test;

import java.util.List;


public class ASynExportTest {

    private DorisJdbcAdapter adapter;
    private String label;

    @Before
    public void setUp() throws Exception {
        JdbcConnectionEntity jdbcConnectionEntity = new JdbcConnectionEntity(DatabaseEnum.DORIS, "192.168.10.202", 9030, "", "root", "123456aA!@");
        adapter = DatabaseAdapterFactory.getAdapter(jdbcConnectionEntity, DorisJdbcAdapter.class);
        label = "1008622";//UUID.randomUUID().toString();
    }


    @Test
    public void aSynExport() throws IllegalAccessException, InstantiationException {
        adapter.aSynExport("bds_log", "bds_asset_info","","","", label);
    }

    @Test
    public void queryASynExportJobStatus() {
        ShowExport showExport = adapter.queryAsynExportJobStatus("bds_log", label);
        System.out.println(showExport);
    }

    @Test
    public void testQueryASynExportJobStatus() {
        List<ShowExport> showExports = adapter.queryAsynExportJobStatus("bds_log");
        System.out.println(showExports);
    }

    @Test
    public void stopASynExportJob() {
        adapter.stopAsynExportJob(label);
    }
}