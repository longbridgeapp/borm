package borm

import (
	"fmt"
	"strings"
)

func queryAnalyzer[T IRow](c *BaseCompoundCondition[T]) string {
	tableName := c.row.GetTableName()
	sql := fmt.Sprintf("SELECT * FROM %s WHERE ", tableName)

	for _, key := range c.fieldValueMap.Keys() {
		element := c.fieldValueMap.GetElement(key)
		sql += fmt.Sprintf("%s=%v AND ", element.Key, element.Value)
	}
	for _, v := range c.inFilterConditions {
		sql += fmt.Sprintf("(%s) IN (%v) AND ", strings.Join(v.fieldNames, ","), v.values)
	}

	sql = strings.TrimRight(sql, "AND ")

	order := "ASC"
	if c.reverse {
		order = "DESC"
	}

	if len(c.sortKey) > 0 {
		sql += fmt.Sprintf(" ORDER BY %s %s", strings.Join(c.sortKey, ","), order)
	} else {
		sql += fmt.Sprintf(" ORDER BY id %s", order)
	}

	if c.limit > 0 || c.offset > 0 {
		sql += fmt.Sprintf(" LIMIT(%v,%v)", c.offset, c.limit)
	}
	return sql
}

func countAnalyzer[T IRow](c *BaseCompoundCondition[T]) string {
	tableName := c.row.GetTableName()
	sql := fmt.Sprintf("SELECT COUNT(id) FROM %s WHERE ", tableName)

	for _, key := range c.fieldValueMap.Keys() {
		element := c.fieldValueMap.GetElement(key)
		sql += fmt.Sprintf("%s=%v AND ", element.Key, element.Value)
	}
	for _, v := range c.inFilterConditions {
		sql += fmt.Sprintf("(%s) IN (%v) AND ", strings.Join(v.fieldNames, ","), v.values)
	}
	sql = strings.TrimRight(sql, "AND ")
	if c.limit > 0 || c.offset > 0 {
		sql += fmt.Sprintf(" LIMIT(%v,%v)", c.offset, c.limit)
	}
	return sql
}
