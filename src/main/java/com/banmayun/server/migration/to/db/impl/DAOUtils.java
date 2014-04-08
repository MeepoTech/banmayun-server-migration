package com.banmayun.server.migration.to.db.impl;

import org.apache.commons.lang3.StringUtils;

public class DAOUtils {

    public static String[] getColumnAliases(String tableAlias, String[] columnNames) {
        String[] ret = new String[columnNames.length];
        for (int i = 0; i < columnNames.length; i++) {
            ret[i] = tableAlias + "_" + columnNames[i];
        }
        return ret;
    }

    public static String getColumnsInsert(String tableAlias, String[] columnNames) {
        return StringUtils.join(columnNames, ", ");
    }

    public static String getColumnsReturn(String tableAlias, String[] columnNames) {
        return StringUtils.remove(getColumnsSelect(tableAlias, columnNames), tableAlias + ".");
    }

    public static String getColumnsSelect(String tableAlias, String[] columnNames) {
        String[] tmp = new String[columnNames.length];
        for (int i = 0; i < columnNames.length; i++) {
            tmp[i] = tableAlias + "." + columnNames[i] + " AS " + tableAlias + "_" + columnNames[i];
        }
        return StringUtils.join(tmp, ", ");
    }

    public static String getColumnsUpdate(String tableAlias, String[] columnNames) {
        String[] tmp = new String[columnNames.length];
        for (int i = 0; i < columnNames.length; i++) {
            tmp[i] = columnNames[i] + "=?";
        }
        return StringUtils.join(tmp, ", ");
    }
}
