package cn.jt.bds.framework.processor.datasource.jdbc.adapter.pojo;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class IntoOutFile {

    private Integer FileNumber;
    private Integer TotalRows;
    private Long FileSize;
    private String URL;
    private String errorMsg;
}
