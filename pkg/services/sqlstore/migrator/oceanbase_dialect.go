package migrator

import (
	_ "github.com/go-sql-driver/mysql"
	"strings"
	"xorm.io/xorm"
)

type OceanBaseDialect struct {
	MySQLDialect
}

func NewOceanBaseDialect(engine *xorm.Engine) Dialect {
	d := OceanBaseDialect{}
	d.BaseDialect.dialect = &d
	d.BaseDialect.engine = engine
	d.BaseDialect.driverName = OceanBase
	return &d
}

func (db *OceanBaseDialect) UpdateTableSQL(tableName string, columns []*Column) string {
	var statements = []string{}

	//statements = append(statements, "DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")

	for _, col := range columns {
		statements = append(statements, "MODIFY "+col.StringNoPk(db))
	}

	return "ALTER TABLE " + db.Quote(tableName) + " " + strings.Join(statements, ", ") + ";"
}
