/* ****************************************************************************
 * MEEPOTECH CONFIDENTIAL
 * ----------------------
 * [2013] - [2014] MeePo Technology Incorporated
 * All Rights Reserved.
 *
 * IMPORTANT NOTICE:
 * All information contained herein is, and remains the property of MeePo
 * Technology Incorporated and its suppliers, if any. The intellectual and
 * technical concepts contained herein are proprietary to MeePo Technology
 * Incorporated and its suppliers and may be covered by Chinese and Foreign
 * Patents, patents in process, and are protected by trade secret or copyright
 * law. Dissemination of this information or reproduction of this material
 * is strictly forbidden unless prior written permission is obtained from
 * MeePo Technology Incorporated.
 * ****************************************************************************
 */

package com.banmayun.server.migration.from.db;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;

import com.banmayun.server.migration.from.core.Group;
import com.banmayun.server.migration.from.core.Relation;
import com.banmayun.server.migration.from.core.User;

public class Filter {

    public enum Field {
        USER_ROLE,
        GROUP_STATUS,
        GROUP_TYPE,
        RELATION_ROLE;
    }

    public enum Operator {
        GEQ,
        GT,
        LEQ,
        LT,
        NEQ,
        EQ;
    }

    public static String toString(Filter[] filters) {
        if (filters == null || filters.length == 0) {
            return null;
        }
        return StringUtils.join(filters, " & ");
    }

    public static String getSQL(Filter[] filters, Set<Filter.Field> fields) {
        if (filters == null || filters.length == 0) {
            return "";
        }
        List<String> effectiveFilters = new ArrayList<String>();
        for (Filter filter : filters) {
            if (!fields.contains(filter.field)) {
                continue;
            }
            effectiveFilters.add(getSQL(filter));
        }
        String sql = StringUtils.join(effectiveFilters, " AND ");
        if (!sql.isEmpty()) {
            return "(" + sql + ") ";
        } else {
            return sql;
        }
    }

    public static final Map<Filter.Field, String> ALIASES;
    static {
        ALIASES = new HashMap<Filter.Field, String>();
        ALIASES.put(Filter.Field.USER_ROLE, "u.role");
        ALIASES.put(Filter.Field.GROUP_TYPE, "g.type");
        ALIASES.put(Filter.Field.GROUP_STATUS, "g.status");
        ALIASES.put(Filter.Field.RELATION_ROLE, "r.role");
    }

    public static final Map<Filter.Field, String> TYPES;
    static {
        TYPES = new HashMap<Filter.Field, String>();
        TYPES.put(Filter.Field.USER_ROLE, "user_role");
        TYPES.put(Filter.Field.GROUP_TYPE, "group_type");
        TYPES.put(Filter.Field.GROUP_STATUS, "group_status");
        TYPES.put(Filter.Field.RELATION_ROLE, "relation_role");
    }

    public static final Map<Filter.Operator, String> OPERATORS;
    static {
        OPERATORS = new HashMap<Filter.Operator, String>();
        OPERATORS.put(Filter.Operator.GT, ">");
        OPERATORS.put(Filter.Operator.LT, "<");
        OPERATORS.put(Filter.Operator.GEQ, ">=");
        OPERATORS.put(Filter.Operator.LEQ, "<=");
        OPERATORS.put(Filter.Operator.EQ, "=");
        OPERATORS.put(Filter.Operator.NEQ, "!=");
    }

    public static String getSQL(Filter filter) {
        String operand = null;
        if (filter.operand instanceof User.Role) {
            operand = ((User.Role) filter.operand).toString().toLowerCase();
        } else if (filter.operand instanceof Group.Type) {
            operand = ((Group.Type) filter.operand).toString().toLowerCase();
        } else if (filter.operand instanceof Group.Status) {
            operand = ((Group.Status) filter.operand).toString().toLowerCase();
        } else if (filter.operand instanceof Relation.Role) {
            operand = ((Relation.Role) filter.operand).toString().toLowerCase();
        } else {
            throw new IllegalArgumentException("unsupported operand type");
        }
        return String.format("%s%s'%s'::%s", ALIASES.get(filter.field), OPERATORS.get(filter.operator), operand,
                TYPES.get(filter.field));
    }

    protected Field field = null;
    protected Operator operator = null;
    protected Object operand = null;

    public Filter() {
    }

    public Filter(Field field, Operator operator, Object operand) {
        this.field = field;
        this.operator = operator;
        this.setOperand(operand);
    }

    public Field getField() {
        return this.field;
    }

    public void setField(Field field) {
        this.field = field;
    }

    public Operator getOperator() {
        return this.operator;
    }

    public void setOperator(Operator operator) {
        this.operator = operator;
    }

    public Object getOperand() {
        return this.operand;
    }

    public void setOperand(Object operand) {
        if (operand == null || operand instanceof User.Role || operand instanceof Group.Type
                || operand instanceof Group.Status || operand instanceof Relation.Role) {
            this.operand = operand;
        } else {
            throw new IllegalArgumentException("unsupported operand type");
        }
    }

    @Override
    public String toString() {
        return this.field + OPERATORS.get(this.operator) + this.operand;
    }
}
