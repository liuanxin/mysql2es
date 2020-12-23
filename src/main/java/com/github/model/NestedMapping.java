package com.github.model;

import com.github.util.A;
import com.github.util.U;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

@Getter
@Setter
public class NestedMapping {

    /**
     * If has table `t_order` and `t_order_item`,
     *
     * `t_order` field has `id`, `t_order_item` relation field has `order_id`,
     *
     * then master-field => id, nested-field => order_id
     */
    private String mainField;

    private String sql;

    private String nestedField;


    public String nestedQuerySql(List<Object> relations) {
        if (U.isBlank(mainField) || U.isBlank(sql) || U.isBlank(nestedField) || A.isEmpty(relations)) {
            return U.EMPTY;
        }

        StringBuilder sbd = new StringBuilder();
        boolean hasQueryRelationInSql = false;
        for (String s : sql.split(" ")) {
            for (String s1 : s.split(",")) {
                if (s1.trim().equals(nestedField.trim())) {
                    hasQueryRelationInSql = true;
                    break;
                }
            }
        }
        if (hasQueryRelationInSql) {
            sbd.append(sql.trim());
        } else {
            sbd.append(sql.trim().replaceFirst("^(?i)SELECT ", "SELECT " + nestedField + ", "));
        }
        sbd.append(" WHERE ").append(nestedField);
        if (relations.size() == 1) {
            sbd.append(" = ");

            Object relation = A.first(relations);
            if (U.isNumber(relation)) {
                sbd.append(relation);
            } else {
                sbd.append("'").append(relation).append("'");
            }
        } else {
            sbd.append(" IN (");

            boolean hasNumber = true;
            for (Object r : relations) {
                if (U.isNotNumber(r)) {
                    hasNumber = false;
                    break;
                }
            }
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
