package database

import (
	"fmt"
	"strings"
	"time"

	"github.com/ch-rollup/ch-rollup/pkg/types"
	sliceUtils "github.com/ch-rollup/ch-rollup/pkg/utils/slice"
	timeUtils "github.com/ch-rollup/ch-rollup/pkg/utils/time"
)

type generateRollUpStatementOptions struct {
	DataBase  string
	FromTable string
	ToTable   string
	Interval  time.Duration
	Columns   []types.ColumnSetting
}

func generateRollUpStatement(opts generateRollUpStatementOptions) string {
	timeColumnName := getTimeColumnName(opts.Columns)

	return fmt.Sprintf("INSERT INTO %s.%s(%s) SELECT %s FROM %s.%s WHERE %s >= $1 AND %s < $2 GROUP BY %s",
		opts.DataBase, opts.ToTable,
		generateInsertColumnsStatement(opts.Columns),
		generateRollupSelectStatement(
			generateIntervalStatement(timeColumnName, opts.Interval),
			opts.Columns,
		),
		opts.DataBase, opts.FromTable,
		timeColumnName, timeColumnName,
		generateGroupByStatement(opts.Columns))
}

func generateInsertColumnsStatement(columns []types.ColumnSetting) string {
	return strings.Join(
		sliceUtils.ConvertFuncWithSkip(
			columns,
			func(elem types.ColumnSetting) (string, bool) {
				return elem.Name, false
			},
		),
		",",
	)
}

func generateRollupSelectStatement(intervalStatement string, columns []types.ColumnSetting) string {
	return strings.Join(
		sliceUtils.ConvertFuncWithSkip(
			columns,
			func(elem types.ColumnSetting) (string, bool) {
				if elem.IsRollUpTime {
					return intervalStatement, false
				}

				if elem.Expression == "" {
					return elem.Name, false
				}

				return elem.Expression, false
			},
		),
		",",
	)
}

func generateGroupByStatement(columns []types.ColumnSetting) string {
	return strings.Join(
		sliceUtils.ConvertFuncWithSkip(
			columns,
			func(elem types.ColumnSetting) (string, bool) {
				return elem.Name, elem.Expression != ""
			},
		),
		",",
	)
}

func generateIntervalStatement(timeColumn string, interval time.Duration) string {
	return fmt.Sprintf("toStartOfInterval(%s, INTERVAL %d SECOND) as %s", timeColumn, timeUtils.SecondsFromDuration(interval), timeColumn)
}

func getTimeColumnName(columns []types.ColumnSetting) string {
	for _, col := range columns {
		if col.IsRollUpTime {
			return col.Name
		}
	}

	return ""
}
