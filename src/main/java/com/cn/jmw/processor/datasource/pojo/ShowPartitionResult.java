package com.cn.jmw.processor.datasource.pojo;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ShowPartitionResult {

    private Long partitionId;
    private String partitionName;
    private Integer visibleVersion;
    private String visibleVersionTime;
    private String state;
    private String partitionKey;
    private String range;
    private String distributionKey;
    private Integer buckets;
    private Integer replicationNum;
    private String storageMedium;
    private String cooldownTime;
    private String remoteStoragePolicy;
    private String lastConsistencyCheckTime;
    private String dataSize;
    private Boolean isInMemory;
    private String replicaAllocation;
    private Boolean isMutable;
    private Boolean syncWithBaseTables;
    private String unsyncTables;
}