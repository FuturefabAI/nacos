/*
 * Copyright 1999-2022 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.nacos.plugin.datasource.impl.oracle;

import com.alibaba.nacos.common.utils.CollectionUtils;
import com.alibaba.nacos.plugin.datasource.constants.DataSourceConstant;
import com.alibaba.nacos.plugin.datasource.constants.FieldConstant;
import com.alibaba.nacos.plugin.datasource.mapper.AbstractMapper;
import com.alibaba.nacos.plugin.datasource.mapper.HistoryConfigInfoMapper;
import com.alibaba.nacos.plugin.datasource.model.MapperContext;
import com.alibaba.nacos.plugin.datasource.model.MapperResult;

import java.util.List;

/**
 * The oracle implementation of HistoryConfigInfoMapper.
 *
 * @author azure_xue
 **/

public class HistoryConfigInfoMapperByOracle extends AbstractMapper implements HistoryConfigInfoMapper {

    @Override
    public MapperResult removeConfigHistory(MapperContext context) {
        String startTime = (String) context.getWhereParameter(FieldConstant.START_TIME);
        int limitSize = (Integer) context.getWhereParameter(FieldConstant.LIMIT_SIZE);

        String sql = "DELETE FROM his_config_info WHERE id IN ("
                + "  SELECT id FROM ("
                + "    SELECT id FROM his_config_info WHERE gmt_modified < ?"
                + "    ORDER BY gmt_modified ASC"
                + "  ) WHERE ROWNUM <= ?"
                + ")";

        return new MapperResult(sql, CollectionUtils.list(startTime, limitSize));
    }

    @Override
    public MapperResult pageFindConfigHistoryFetchRows(MapperContext context) {
        int startRow = context.getStartRow() + 1;
        int endRow = startRow + context.getPageSize() - 1;

        String tenantId = (String) context.getWhereParameter(FieldConstant.TENANT_ID);
        List<Object> paramsList = CollectionUtils.list(
                context.getWhereParameter(FieldConstant.DATA_ID),
                context.getWhereParameter(FieldConstant.GROUP_ID),
                context.getWhereParameter(FieldConstant.TENANT_ID)
        );
        String sql = "SELECT * FROM ("
                + "  SELECT nid, data_id, group_id, tenant_id, app_name, src_ip, src_user, op_type, gmt_create, gmt_modified, ROWNUM rnum"
                + "  FROM his_config_info"
                + "  WHERE data_id = ? AND group_id = ? AND tenant_id = ?"
                + "  ORDER BY nid DESC"
                + ") WHERE rnum BETWEEN " + startRow + " AND " + endRow;

        if (tenantId.isEmpty()) {
            sql = "SELECT * FROM ("
                    + "  SELECT nid, data_id, group_id, tenant_id, app_name, src_ip, src_user, op_type, gmt_create, gmt_modified, ROWNUM rnum"
                    + "  FROM his_config_info"
                    + "  WHERE data_id = ? AND group_id = ? AND tenant_id is NULL"
                    + "  ORDER BY nid DESC"
                    + ") WHERE rnum BETWEEN " + startRow + " AND " + endRow;
            paramsList.remove(context.getWhereParameter(FieldConstant.TENANT_ID));
        }

        return new MapperResult(sql, paramsList);
    }

    @Override
    public String getDataSource() {
        return DataSourceConstant.ORACLE;
    }
}
