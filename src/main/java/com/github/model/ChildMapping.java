package com.github.model;

import com.github.util.A;
import com.github.util.U;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

/**
 * If has table `t_order` and `t_order_item`,
 *
 * `t_order` field has `id`, `t_order_item` relation field has `order_id`,
 *
 * then master-field => id, child-field => order_id
 */
@Getter
@Setter
public class ChildMapping {

    private String mainField;

    private String sql;

    private String childField;


    public void check(String nested) {
        U.assertNil(mainField, "must set child(" + nested + "): main-field");
        U.assertNil(sql, "must set child(" + nested + "): sql");
        U.assertNil(childField, "must set child(" + nested + "): nested-field");
    }
    public String nestedQuerySql(List<Object> relations) {
        if (A.isEmpty(relations)) {
            return U.EMPTY;
        }

        StringBuilder sbd = new StringBuilder();
        boolean hasQueryRelationInSql = false;
        for (String s : sql.split(" ")) {
            for (String s1 : s.split(",")) {
                if (s1.trim().equals(childField.trim())) {
                    hasQueryRelationInSql = true;
                    break;
                }
            }
        }
        if (hasQueryRelationInSql) {
            sbd.append(sql.trim());
        } else {
            sbd.append(sql.trim().replaceFirst("^(?i)SELECT ", "SELECT " + childField + ", "));
        }
        sbd.append(" WHERE ").append(childField);
        Object relation = A.first(relations);
        if (relations.size() == 1) {
            sbd.append(" = ");
            if (U.isNumber(relation)) {
                sbd.append(relation);
            } else {
                sbd.append("'").append(relation).append("'");
            }
        } else {
            sbd.append(" IN (");
            boolean hasNumber = U.isNumber(relation);
            for (int i = 0; i < relations.size(); i++) {
                if (i > 0) {
                    sbd.append(",");
                }
                Object r = relations.get(i);
                if (hasNumber) {
                    sbd.append(r);
                } else {
                    sbd.append("'").append(r).append("'");
                }
            }
            sbd.append(")");
        }
        return sbd.toString();
    }
}
