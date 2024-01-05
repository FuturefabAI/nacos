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

import com.alibaba.nacos.plugin.datasource.constants.DataSourceConstant;
import com.alibaba.nacos.plugin.datasource.mapper.AbstractMapper;
import com.alibaba.nacos.plugin.datasource.mapper.ConfigInfoTagMapper;
import com.alibaba.nacos.plugin.datasource.model.MapperContext;
import com.alibaba.nacos.plugin.datasource.model.MapperResult;

import java.util.Collections;
/**
 * The oracle implementation of ConfigInfoTagMapper.
 *
 * @author azure_xue
 **/

public class ConfigInfoTagMapperByOracle extends AbstractMapper implements ConfigInfoTagMapper {

    @Override
    public MapperResult findAllConfigInfoTagForDumpAllFetchRows(MapperContext context) {
        int startRow = context.getStartRow() + 1;
        int endRow = startRow + context.getPageSize() - 1;

        String sql = "SELECT t.id, t.data_id, t.group_id, t.tenant_id, t.tag_id, t.app_name, t.content, t.md5, t.gmt_modified "
                + "FROM ( "
                + "  SELECT a.id, a.data_id, a.group_id, a.tenant_id, a.tag_id, a.app_name, a.content, a.md5, a.gmt_modified, ROWNUM rnum "
                + "  FROM config_info_tag a "
                + "  ORDER BY a.id "
                + ") t "
                + "WHERE t.rnum BETWEEN " + startRow + " AND " + endRow;
        return new MapperResult(sql, Collections.emptyList());
    }

    @Override
    public String getDataSource() {
        return DataSourceConstant.ORACLE;
    }
}

