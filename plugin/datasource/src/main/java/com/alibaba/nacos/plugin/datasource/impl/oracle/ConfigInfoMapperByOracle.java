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
import com.alibaba.nacos.common.utils.NamespaceUtil;
import com.alibaba.nacos.common.utils.StringUtils;
import com.alibaba.nacos.plugin.datasource.constants.DataSourceConstant;
import com.alibaba.nacos.plugin.datasource.constants.FieldConstant;
import com.alibaba.nacos.plugin.datasource.mapper.AbstractMapper;
import com.alibaba.nacos.plugin.datasource.mapper.ConfigInfoMapper;
import com.alibaba.nacos.plugin.datasource.model.MapperContext;
import com.alibaba.nacos.plugin.datasource.model.MapperResult;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * The mysql implementation of ConfigInfoMapper.
 *
 * @author hyx
 **/

public class ConfigInfoMapperByOracle extends AbstractMapper implements ConfigInfoMapper {

    private static final String DATA_ID = "dataId";

    private static final String GROUP = "group";

    private static final String APP_NAME = "appName";

    private static final String CONTENT = "content";

    private static final String TENANT = "tenant";

    @Override
    public MapperResult findConfigInfoByAppFetchRows(MapperContext context) {
        final String appName = (String) context.getWhereParameter(FieldConstant.APP_NAME);
        final String tenantId = (String) context.getWhereParameter(FieldConstant.TENANT_ID);
        int startRow = context.getStartRow();
        int pageSize = context.getPageSize();
        String sql = "SELECT * FROM ("
                + "    SELECT id, data_id, group_id, tenant_id, app_name, content, ROWNUM as rnum FROM config_info"
                + "    WHERE tenant_id LIKE ? AND app_name = ?"
                + ") WHERE rnum BETWEEN " + startRow + " AND " + (startRow + pageSize - 1);
        if (tenantId.isEmpty()) {
            sql = "SELECT * FROM ("
                    + "    SELECT id, data_id, group_id, tenant_id, app_name, content, ROWNUM as rnum FROM config_info"
                    + "    WHERE tenant_id is NULL AND app_name = ?"
                    + ") WHERE rnum BETWEEN " + startRow + " AND " + (startRow + pageSize - 1);
        }
        return new MapperResult(sql, CollectionUtils.list(tenantId, appName));
    }

    @Override
    public MapperResult getTenantIdList(MapperContext context) {
        String sql = "SELECT * FROM ("
                + "  SELECT tenant_id, ROWNUM as rnum"
                + "  FROM (SELECT tenant_id FROM config_info WHERE tenant_id != '" + NamespaceUtil.getNamespaceDefaultId() + "' GROUP BY tenant_id)"
                + ")"
                + " WHERE rnum BETWEEN " + (context.getStartRow() + 1) + " AND " + (context.getStartRow() + context.getPageSize());
        return new MapperResult(sql, Collections.emptyList());
    }

    @Override
    public MapperResult getGroupIdList(MapperContext context) {
        String sql = "SELECT * FROM ("
                + "  SELECT group_id, ROWNUM AS rnum"
                + "  FROM (SELECT group_id FROM config_info WHERE tenant_id is NULL GROUP BY group_id)"
                + ")"
                + " WHERE rnum BETWEEN " + (context.getStartRow() + 1)
                + " AND " + (context.getStartRow() + context.getPageSize());
        // String sql = "SELECT * FROM ("
        //         + "  SELECT group_id, ROWNUM AS rnum"
        //         + "  FROM (SELECT group_id FROM config_info WHERE tenant_id = '"
        //         + NamespaceUtil.getNamespaceDefaultId()
        //         + "' GROUP BY group_id)"
        //         + ")"
        //         + " WHERE rnum BETWEEN " + (context.getStartRow() + 1)
        //         + " AND " + (context.getStartRow() + context.getPageSize());
        return new MapperResult(sql, Collections.emptyList());
    }

    @Override
    public MapperResult findAllConfigKey(MapperContext context) {
        String tenantId = (String) context.getWhereParameter(FieldConstant.TENANT_ID);
        String sql = "SELECT data_id, group_id, app_name FROM ("
                + "  SELECT t.id, t.data_id, t.group_id, t.app_name, ROWNUM as rnum FROM config_info t"
                + "  WHERE tenant_id LIKE ?"
                + "  ORDER BY t.id"
                + ") WHERE rnum BETWEEN " + (context.getStartRow() + 1) + " AND " + (context.getStartRow() + context.getPageSize());
        if (StringUtils.isBlank(tenantId)) {
            sql = "SELECT data_id, group_id, app_name FROM ("
                    + "  SELECT t.id, t.data_id, t.group_id, t.app_name, ROWNUM as rnum FROM config_info t"
                    + "  WHERE tenant_id is NULL"
                    + "  ORDER BY t.id"
                    + ") WHERE rnum BETWEEN " + (context.getStartRow() + 1) + " AND " + (context.getStartRow() + context.getPageSize());
        }
        return new MapperResult(sql, CollectionUtils.list(context.getWhereParameter(FieldConstant.TENANT_ID)));
    }

    @Override
    public MapperResult findAllConfigInfoBaseFetchRows(MapperContext context) {
        String appName = (String) context.getWhereParameter(FieldConstant.APP_NAME);
        final String tenantId = (String) context.getWhereParameter(FieldConstant.TENANT_ID);
        String sql = "SELECT t.id, t.data_id, t.group_id, t.content, t.md5"
                + " FROM ("
                + "    SELECT id, data_id, group_id, content, md5, ROWNUM rnum"
                + "    FROM config_info"
                + "    ORDER BY id"
                + " ) t"
                + " WHERE rnum BETWEEN ? AND ?";
        return new MapperResult(sql, Collections.emptyList());
    }

    @Override
    public MapperResult findAllConfigInfoFragment(MapperContext context) {
        String sql = "SELECT * FROM ("
                + "  SELECT a.*, ROWNUM rnum FROM ("
                + "    SELECT id, data_id, group_id, tenant_id, app_name, content, md5, gmt_modified, type, encrypted_data_key"
                + "    FROM config_info"
                + "    WHERE id > ?"
                + "    ORDER BY id ASC"
                + "  ) a"
                + "  WHERE ROWNUM <= " + (context.getStartRow() + context.getPageSize())
                + ") WHERE rnum > " + context.getStartRow();
        return new MapperResult(sql, CollectionUtils.list(context.getWhereParameter(FieldConstant.ID)));
    }

    @Override
    public MapperResult findChangeConfigFetchRows(MapperContext context) {
        final String tenant = (String) context.getWhereParameter(FieldConstant.TENANT_ID);
        final String dataId = (String) context.getWhereParameter(FieldConstant.DATA_ID);
        final String group = (String) context.getWhereParameter(FieldConstant.GROUP_ID);
        final String appName = (String) context.getWhereParameter(FieldConstant.APP_NAME);
        final String tenantTmp = StringUtils.isBlank(tenant) ? StringUtils.EMPTY : tenant;
        final Timestamp startTime = (Timestamp) context.getWhereParameter(FieldConstant.START_TIME);
        final Timestamp endTime = (Timestamp) context.getWhereParameter(FieldConstant.END_TIME);

        List<Object> paramList = new ArrayList<>();

        final String sqlFetchRows = "SELECT * FROM (SELECT id, data_id, group_id, tenant_id, app_name, content, type, "
                + "md5, gmt_modified, ROWNUM rnum FROM config_info WHERE ";
        String where = " 1=1 ";
        if (!StringUtils.isBlank(dataId)) {
            where += " AND data_id LIKE ? ";
            paramList.add(dataId);
        }
        if (!StringUtils.isBlank(group)) {
            where += " AND group_id LIKE ? ";
            paramList.add(group);
        }

        if (!StringUtils.isBlank(tenantTmp)) {
            where += " AND tenant_id = ? ";
            paramList.add(tenantTmp);
        } else {
            where += " AND tenant_id IS NULL ";
        }

        if (!StringUtils.isBlank(appName)) {
            where += " AND app_name = ? ";
            paramList.add(appName);
        }
        if (startTime != null) {
            where += " AND gmt_modified >= ? ";
            paramList.add(startTime);
        }
        if (endTime != null) {
            where += " AND gmt_modified <= ? ";
            paramList.add(endTime);
        }

        int startRow = 0;
        int endRow = startRow + context.getPageSize();

        String finalSql = sqlFetchRows + where + " AND id > " + context.getWhereParameter(FieldConstant.LAST_MAX_ID)
                + " ORDER BY id ASC) WHERE rnum BETWEEN " + (startRow + 1) + " AND " + endRow;
        return new MapperResult(finalSql, paramList);
    }

    @Override
    public MapperResult listGroupKeyMd5ByPageFetchRows(MapperContext context) {
        int startRow = context.getStartRow();
        int endRow = startRow + context.getPageSize();

        String sql = "SELECT t.id, t.data_id, t.group_id, t.tenant_id, t.app_name, t.md5, t.type, t.gmt_modified, t.encrypted_data_key FROM "
                + "( SELECT id, data_id, group_id, tenant_id, app_name, md5, type, gmt_modified, encrypted_data_key, ROWNUM rnum FROM config_info "
                + "ORDER BY id ) t "
                + "WHERE t.rnum > " + startRow + " AND t.rnum <= " + endRow + " AND t.id = t.id";
        return new MapperResult(sql, Collections.emptyList());
    }

    @Override
    public MapperResult findConfigInfoBaseLikeFetchRows(MapperContext context) {
        final String dataId = (String) context.getWhereParameter(FieldConstant.DATA_ID);
        final String group = (String) context.getWhereParameter(FieldConstant.GROUP_ID);
        final String content = (String) context.getWhereParameter(FieldConstant.CONTENT);

        String where = " 1=1 AND tenant_id='" + NamespaceUtil.getNamespaceDefaultId() + "' ";
        List<Object> paramList = new ArrayList<>();

        if (!StringUtils.isBlank(dataId)) {
            where += " AND data_id LIKE ? ";
            paramList.add(dataId);
        }
        if (!StringUtils.isBlank(group)) {
            where += " AND group_id LIKE ? ";
            paramList.add(group);
        }
        if (!StringUtils.isBlank(content)) {
            where += " AND content LIKE ? ";
            paramList.add(content);
        }

        int startRow = context.getStartRow() + 1;
        int endRow = startRow + context.getPageSize() - 1;

        final String sqlFetchRows = "SELECT * FROM ("
                + " SELECT a.*, ROWNUM rnum FROM ("
                + "     SELECT id, data_id, group_id, tenant_id, content FROM config_info WHERE "
                + where
                + "     ORDER BY id"
                + " ) a"
                + " WHERE ROWNUM <= " + endRow
                + ") WHERE rnum >= " + startRow;

        return new MapperResult(sqlFetchRows, paramList);
    }

    @Override
    public MapperResult findConfigInfo4PageFetchRows(MapperContext context) {
        final String tenant = (String) context.getWhereParameter(FieldConstant.TENANT_ID);
        final String dataId = (String) context.getWhereParameter(FieldConstant.DATA_ID);
        final String group = (String) context.getWhereParameter(FieldConstant.GROUP_ID);
        final String appName = (String) context.getWhereParameter(FieldConstant.APP_NAME);
        final String content = (String) context.getWhereParameter(FieldConstant.CONTENT);

        List<Object> paramList = new ArrayList<>();

        final String sql = "SELECT * FROM (SELECT id, data_id, group_id, tenant_id, app_name, content, type, "
                + "encrypted_data_key, ROWNUM rnum FROM config_info";
        StringBuilder where = new StringBuilder(" WHERE ");
        where.append(" tenant_id=? ");
        paramList.add(tenant);
        if (StringUtils.isNotBlank(dataId)) {
            where.append(" AND data_id=? ");
            paramList.add(dataId);
        }
        if (StringUtils.isNotBlank(group)) {
            where.append(" AND group_id=? ");
            paramList.add(group);
        }
        if (StringUtils.isNotBlank(appName)) {
            where.append(" AND app_name=? ");
            paramList.add(appName);
        }
        if (!StringUtils.isBlank(content)) {
            where.append(" AND content LIKE ? ");
            paramList.add(content);
        }

        int startRow = context.getStartRow() + 1;
        int endRow = startRow + context.getPageSize() - 1;

        return new MapperResult(sql + where + ") WHERE rnum BETWEEN " + startRow + " AND " + endRow, paramList);
    }

    @Override
    public MapperResult findConfigInfoBaseByGroupFetchRows(MapperContext context) {
        String sql = "SELECT * FROM ("
                + "  SELECT id, data_id, group_id, content, ROWNUM rnum"
                + "  FROM config_info"
                + "  WHERE group_id=? AND tenant_id=?"
                + "  ORDER BY id"
                + ") WHERE rnum BETWEEN " + (context.getStartRow() + 1)
                + " AND " + (context.getStartRow() + context.getPageSize());

        return new MapperResult(sql, CollectionUtils.list(
                context.getWhereParameter(FieldConstant.GROUP_ID),
                context.getWhereParameter(FieldConstant.TENANT_ID)
        ));
    }

    /**
     * Query config info count. The default sql: SELECT count(*) FROM config_info ...
     *
     * @param context The map of dataId, group, appName, content
     * @return The sql of querying config info count
     */
    @Override
    public MapperResult findConfigInfoLike4PageCountRows(MapperContext context) {
        final String tenant = (String) context.getWhereParameter(FieldConstant.TENANT_ID);
        final String dataId = (String) context.getWhereParameter(FieldConstant.DATA_ID);
        final String group = (String) context.getWhereParameter(FieldConstant.GROUP_ID);
        final String content = (String) context.getWhereParameter(FieldConstant.CONTENT);
        final String appName = (String) context.getWhereParameter(FieldConstant.APP_NAME);
        final String tenantId = (String) context.getWhereParameter(FieldConstant.TENANT_ID);

        final List<Object> paramList = new ArrayList<>();

        final String sqlCountRows = "SELECT count(*) FROM config_info";
        StringBuilder where = new StringBuilder(" WHERE ");
        if (StringUtils.isBlank(tenant)) {
            where.append(" tenant_id IS NULL ");
        } else {
            where.append(" tenant_id LIKE ? ");
            paramList.add(tenantId);
        }

        if (!StringUtils.isBlank(dataId)) {
            where.append(" AND data_id LIKE ? ");
            paramList.add(dataId);
        }
        if (!StringUtils.isBlank(group)) {
            where.append(" AND group_id LIKE ? ");
            paramList.add(group);
        }
        if (!StringUtils.isBlank(appName)) {
            where.append(" AND app_name = ? ");
            paramList.add(appName);
        }
        if (!StringUtils.isBlank(content)) {
            where.append(" AND content LIKE ? ");
            paramList.add(content);
        }
        return new MapperResult(sqlCountRows + where, paramList);
    }

    @Override
    public MapperResult findConfigInfoLike4PageFetchRows(MapperContext context) {
        final String tenant = (String) context.getWhereParameter(FieldConstant.TENANT_ID);
        final String dataId = (String) context.getWhereParameter(FieldConstant.DATA_ID);
        final String group = (String) context.getWhereParameter(FieldConstant.GROUP_ID);
        final String appName = (String) context.getWhereParameter(FieldConstant.APP_NAME);
        final String content = (String) context.getWhereParameter(FieldConstant.CONTENT);

        List<Object> paramList = new ArrayList<>();

        StringBuilder where;
        if (StringUtils.isBlank(tenant)) {
            where = new StringBuilder(" WHERE tenant_id IS NULL ");
        } else {
            where = new StringBuilder(" WHERE tenant_id LIKE ? ");
            paramList.add(tenant);
        }
        if (!StringUtils.isBlank(dataId)) {
            where.append(" AND data_id LIKE ? ");
            paramList.add(dataId);
        }
        if (!StringUtils.isBlank(group)) {
            where.append(" AND group_id LIKE ? ");
            paramList.add(group);
        }
        if (!StringUtils.isBlank(appName)) {
            where.append(" AND app_name = ? ");
            paramList.add(appName);
        }
        if (!StringUtils.isBlank(content)) {
            where.append(" AND content LIKE ? ");
            paramList.add(content);
        }

        int startRow = context.getStartRow() + 1;
        int endRow = startRow + context.getPageSize() - 1;

        final String sqlFetchRows = "SELECT * FROM ("
                + " SELECT id, data_id, group_id, tenant_id, app_name, content, encrypted_data_key, ROWNUM rnum FROM config_info"
                + where
                + " ORDER BY id )"
                + " WHERE rnum BETWEEN " + startRow + " AND " + endRow;

        return new MapperResult(sqlFetchRows, paramList);
    }

    @Override
    public MapperResult findAllConfigInfoFetchRows(MapperContext context) {
        String tenantId = (String) context.getWhereParameter(FieldConstant.TENANT_ID);
        String sql = "SELECT t.id, t.data_id, t.group_id, t.tenant_id, t.app_name, t.content, t.md5 "
                + "FROM ("
                + "    SELECT a.id, a.data_id, a.group_id, a.tenant_id, a.app_name, a.content, a.md5, ROWNUM rnum"
                + "    FROM config_info a"
                + "    WHERE a.tenant_id LIKE ?"
                + "    ORDER BY a.id"
                + ") t"
                + " WHERE t.rnum BETWEEN ? AND ?";
        if (StringUtils.isBlank(tenantId)) {
            sql = "SELECT t.id, t.data_id, t.group_id, t.tenant_id, t.app_name, t.content, t.md5 "
                    + "FROM ("
                    + "    SELECT a.id, a.data_id, a.group_id, a.tenant_id, a.app_name, a.content, a.md5, ROWNUM rnum"
                    + "    FROM config_info a"
                    + "    WHERE a.tenant_id is NULL"
                    + "    ORDER BY a.id"
                    + ") t"
                    + " WHERE t.rnum BETWEEN ? AND ?";
        }
        int startRow = context.getStartRow() + 1;
        int endRow = startRow + context.getPageSize() - 1;

        return new MapperResult(sql, CollectionUtils.list(
                context.getWhereParameter(FieldConstant.TENANT_ID), startRow, endRow
        ));
    }

    @Override
    public String getDataSource() {
        return DataSourceConstant.ORACLE;
    }

}
