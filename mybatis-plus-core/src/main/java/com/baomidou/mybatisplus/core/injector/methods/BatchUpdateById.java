package com.baomidou.mybatisplus.core.injector.methods;

import com.baomidou.mybatisplus.annotation.FieldFill;
import com.baomidou.mybatisplus.core.enums.SqlMethod;
import com.baomidou.mybatisplus.core.exceptions.MybatisPlusException;
import com.baomidou.mybatisplus.core.injector.AbstractMethod;
import com.baomidou.mybatisplus.core.metadata.TableFieldInfo;
import com.baomidou.mybatisplus.core.metadata.TableInfo;
import com.baomidou.mybatisplus.core.toolkit.CollectionUtils;
import com.baomidou.mybatisplus.core.toolkit.Constants;
import com.baomidou.mybatisplus.core.toolkit.sql.SqlScriptUtils;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.mapping.SqlSource;

import java.util.Collection;
import java.util.Objects;

import static com.baomidou.mybatisplus.core.toolkit.StringPool.NEWLINE;

/**
 * Created by IntelliJ IDEA.
 * User: dongxurr123
 * Date: 18-8-29
 * Time: 下午11:12
 */
public class BatchUpdateById extends AbstractMethod {

    private final static String BATCH_UPDATE_BY_KEYS_ONE_FIELD =
            "<foreach collection=\"" + Constants.COLLECTION + "\" item=\"" + Constants.ENTITY + "\" separator=\" \" open=\"CASE %s\" close=\"END\">" + NEWLINE +
                "WHEN %s THEN " + NEWLINE +
                "<choose>" + NEWLINE +
                    "<when test=\"%s==null\">" + NEWLINE +
                        "%s" + NEWLINE +
                    "</when>" + NEWLINE +
                    "<otherwise>" + NEWLINE +
                        "%s" + NEWLINE +
                    "</otherwise>" + NEWLINE +
                "</choose>" + NEWLINE +
            "</foreach>" + NEWLINE;

    private final static String BATCH_UPDATE_WHERE =
            "<foreach collection=\"" + Constants.COLLECTION + "\" index=\"index\" item=\"item\" separator=\",\" open=\"(\" close=\")\">" + NEWLINE +
                "#{item.%s}" + NEWLINE +
            "</foreach>" + NEWLINE;

    @Override
    public MappedStatement injectMappedStatement(Class<?> mapperClass, Class<?> modelClass, TableInfo tableInfo) {
        final SqlMethod sqlMethod = SqlMethod.BATCH_UPDATE_BY_ID;

        if (CollectionUtils.isEmpty(tableInfo.getFieldList())) {
            throw new MybatisPlusException("tableInfo contains no field, tableName:" + tableInfo.getTableName());
        }

        Collection<TableFieldInfo> tableFieldInfos = tableInfo.getFieldList();
        StringBuilder sb = new StringBuilder("<script><![CDATA[UPDATE `").append(tableInfo.getTableName()).append("` SET ]]>").append(NEWLINE);
        final String keyColumn = tableInfo.getKeyColumn();
        final String keyProperty = tableInfo.getKeyProperty();
        int idx = 0;
        for (TableFieldInfo tableFieldInfo : tableFieldInfos) {
            if (Objects.equals(tableFieldInfo.getColumn(), keyColumn) || tableFieldInfo.getFieldFill() == FieldFill.INSERT) {
                // 字段在这两种情况下跳过拼接update sql: 1.主键字段, 2.插入时更新的字段
                continue;
            }
            sb.append("<![CDATA[");
            if (idx++ > 0) {
                sb.append(',');
            }
            if (tableFieldInfo.getFieldFill() == FieldFill.UPDATE || tableFieldInfo.getFieldFill() == FieldFill.INSERT_UPDATE) {
                //
                sb.append(String.format("`%s`=%s]]>", tableFieldInfo.getColumn(), SqlScriptUtils.safeParam(Constants.ENTITY_SPOT + tableFieldInfo.getEl()))).append(NEWLINE);
            }
            else {
                sb.append(String.format("`%s`=]]>", tableFieldInfo.getColumn())).append(NEWLINE);
                sb.append(String.format(BATCH_UPDATE_BY_KEYS_ONE_FIELD,
                    keyColumn,
                    SqlScriptUtils.safeParam(Constants.ENTITY_SPOT + keyProperty),
                    SqlScriptUtils.safeParam(Constants.ENTITY_SPOT + tableFieldInfo.getProperty()),
                    tableFieldInfo.getColumn(),
                    SqlScriptUtils.safeParam(Constants.ENTITY_SPOT + tableFieldInfo.getProperty())
                ));
            }
        }
        sb.append("<![CDATA[ WHERE ").append(keyColumn).append(" IN ]]>");
        sb.append(String.format(BATCH_UPDATE_WHERE, keyProperty));

        sb.append("</script>");

        SqlSource sqlSource = languageDriver.createSqlSource(configuration, sb.toString(), modelClass);
        return addUpdateMappedStatement(mapperClass, modelClass, sqlMethod.getMethod(), sqlSource);
    }
}
