package com.cn.jmw.processor.datasource.jdbc.adapter;

import com.cn.jmw.processor.datasource.enums.DatabaseEnum;
import com.cn.jmw.processor.datasource.factory.DatabaseAdapterFactory;
import com.cn.jmw.processor.datasource.jdbc.inter.doris.pojo.ProcessListPojo;
import com.cn.jmw.processor.datasource.pojo.JDBCConnectionEntity;
import com.cn.jmw.processor.datasource.pojo.ShowCreateTable;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class DorisJDBCAdapterTest {

    private DorisJDBCAdapter adapter;

    @Before
    public void setUp() throws Exception {
        JDBCConnectionEntity jdbcConnectionEntity = new JDBCConnectionEntity(DatabaseEnum.DORIS, "192.168.10.202", 9030, "", "root", "123456aA!@");
        adapter = DatabaseAdapterFactory.getAdapter(jdbcConnectionEntity, DorisJDBCAdapter.class);
    }

    @Test
    public void getCreateTableDDL() {
        ShowCreateTable createTableDDL = adapter.getCreateTableDDL("bds_log", "bds_asset_info");
        System.out.println(createTableDDL);
    }

    @Test
    public void getProcessList(){
        List<ProcessListPojo> processList = adapter.getProcessList(new ProcessListPojo());
        System.out.println(processList);
    }

    @Test
    public void killQuery(){
        adapter.killQuery("f25d1282741c4e81-a7742a6136e3e84d");
    }

    @Test
    public void killConnection(){
        adapter.killConnection(45010);
    }
}