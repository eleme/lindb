// Licensed to LinDB under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. LinDB licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package sql

import (
	"github.com/lindb/lindb/sql/grammar"
	"github.com/lindb/lindb/sql/stmt"
)

type listener struct {
	*grammar.BaseSQLListener

	queryStmt   *queryStmtParser
	stateStmt   *stateStmtParser
	metaStmt    *metaStmtParser
	useStmt     *useStmtParser
	schemasStmt *schemasStmtParser
	storageStmt *storageStmtParser
}

// EnterQueryStmt is called when production queryStmt is entered.
func (l *listener) EnterQueryStmt(ctx *grammar.QueryStmtContext) {
	l.queryStmt = newQueryStmtParse(ctx.T_EXPLAIN() != nil)
}

// EnterShowMasterStmt is called when production showMasterStmt is entered.
func (l *listener) EnterShowMasterStmt(_ *grammar.ShowMasterStmtContext) {
	l.stateStmt = newStateStmtParse(stmt.Master)
}

// EnterShowAliveStmt is called when production showAliveStmt is entered.
func (l *listener) EnterShowAliveStmt(ctx *grammar.ShowAliveStmtContext) {
	switch {
	case ctx.T_BROKER() != nil:
		l.stateStmt = newStateStmtParse(stmt.BrokerAlive)
	case ctx.T_STORAGE() != nil:
		l.stateStmt = newStateStmtParse(stmt.StorageAlive)
	}
}

// EnterShowBrokerMetricStmt is called when production showBrokerMetricStmt is entered.
func (l *listener) EnterShowBrokerMetricStmt(_ *grammar.ShowBrokerMetricStmtContext) {
	l.stateStmt = newStateStmtParse(stmt.BrokerMetric)
}

// EnterShowStorageMetricStmt is called when production showStorageMetricStmt is entered.
func (l *listener) EnterShowStorageMetricStmt(_ *grammar.ShowStorageMetricStmtContext) {
	l.stateStmt = newStateStmtParse(stmt.StorageMetric)
}

// EnterMetricList is called when production metricList is entered.
func (l *listener) EnterMetricList(ctx *grammar.MetricListContext) {
	l.stateStmt.visitMetricList(ctx)
}

// EnterShowReplicationStmt is called when production showReplicationStmt is entered.
func (l *listener) EnterShowReplicationStmt(_ *grammar.ShowReplicationStmtContext) {
	l.stateStmt = newStateStmtParse(stmt.Replication)
}

// EnterStorageFilter is called when production storageFilter is entered.
func (l *listener) EnterStorageFilter(ctx *grammar.StorageFilterContext) {
	l.stateStmt.visitStorageFilter(ctx)
}

// EnterDatabaseFilter is called when production databaseFilter is entered.
func (l *listener) EnterDatabaseFilter(ctx *grammar.DatabaseFilterContext) {
	l.stateStmt.visitDatabaseFilter(ctx)
}

// EnterShowStoragesStmt is called when production showStoragesStmt is entered.
func (l *listener) EnterShowStoragesStmt(_ *grammar.ShowStoragesStmtContext) {
	l.storageStmt = newStorageStmtParse(stmt.StorageOpShow)
}

// EnterJson is called when production json is entered.
func (l *listener) EnterJson(ctx *grammar.JsonContext) { // nolint:golint
	if l.storageStmt != nil {
		l.storageStmt.visitCfg(ctx)
	}
}

// EnterCreateStorageStmt is called when production createStorageStmt is entered.
func (l *listener) EnterCreateStorageStmt(_ *grammar.CreateStorageStmtContext) {
	l.storageStmt = newStorageStmtParse(stmt.StorageOpCreate)
}

// EnterCreateDatabaseStmt is called when entering the createDatabaseStmt production.
func (l *listener) EnterCreateDatabaseStmt(_ *grammar.CreateDatabaseStmtContext) {
	panic("need impl")
}

// EnterShowSchemasStmt is called when production showSchemasStmt is entered.
func (l *listener) EnterShowSchemasStmt(_ *grammar.ShowSchemasStmtContext) {
	l.schemasStmt = newSchemasStmtParse(stmt.DatabaseSchemaType)
}

// EnterUseStmt is called when production useStmt is entered.
func (l *listener) EnterUseStmt(ctx *grammar.UseStmtContext) {
	l.useStmt = newUseStmtParse()
	l.useStmt.visitName(ctx.Ident())
}

// EnterShowDatabaseStmt is called when production showDatabaseStmt is entered.
func (l *listener) EnterShowDatabaseStmt(_ *grammar.ShowDatabaseStmtContext) {
	l.schemasStmt = newSchemasStmtParse(stmt.DatabaseNameSchemaType)
}

// EnterShowNameSpacesStmt is called when production showNameSpacesStmt is entered.
func (l *listener) EnterShowNameSpacesStmt(_ *grammar.ShowNameSpacesStmtContext) {
	l.metaStmt = newMetaStmtParser(stmt.Namespace)
}

// EnterShowMetricsStmt is called when production showMetricsStmt is entered.
func (l *listener) EnterShowMetricsStmt(_ *grammar.ShowMetricsStmtContext) {
	l.metaStmt = newMetaStmtParser(stmt.Metric)
}

// EnterShowFieldsStmt is called when production showFieldsStmt is entered.
func (l *listener) EnterShowFieldsStmt(_ *grammar.ShowFieldsStmtContext) {
	l.metaStmt = newMetaStmtParser(stmt.Field)
}

// EnterShowTagKeysStmt is called when production showTagKeysStmt is entered.
func (l *listener) EnterShowTagKeysStmt(_ *grammar.ShowTagKeysStmtContext) {
	l.metaStmt = newMetaStmtParser(stmt.TagKey)
}

// EnterShowTagValuesStmt is called when production showTagValuesStmt is entered.
func (l *listener) EnterShowTagValuesStmt(_ *grammar.ShowTagValuesStmtContext) {
	l.metaStmt = newMetaStmtParser(stmt.TagValue)
}

// EnterNamespace is called when production namespace is entered.
func (l *listener) EnterNamespace(ctx *grammar.NamespaceContext) {
	switch {
	case l.queryStmt != nil:
		l.queryStmt.visitNamespace(ctx)
	case l.metaStmt != nil:
		l.metaStmt.visitNamespace(ctx)
	}
}

// EnterWithTagKey is called when production withTagKey is entered.
func (l *listener) EnterWithTagKey(ctx *grammar.WithTagKeyContext) {
	if l.metaStmt != nil {
		l.metaStmt.visitWithTagKey(ctx)
	}
}

// EnterPrefix is called when production prefix is entered.
func (l *listener) EnterPrefix(ctx *grammar.PrefixContext) {
	if l.metaStmt != nil {
		l.metaStmt.visitPrefix(ctx)
	}
}

// EnterMetricName is called when production metricName is entered.
func (l *listener) EnterMetricName(ctx *grammar.MetricNameContext) {
	switch {
	case l.queryStmt != nil:
		l.queryStmt.visitMetricName(ctx)
	case l.metaStmt != nil:
		l.metaStmt.visitMetricName(ctx)
	}
}

// EnterSelectExpr is called when production selectExpr is entered.
func (l *listener) EnterSelectExpr(_ *grammar.SelectExprContext) {
	if l.queryStmt != nil {
		l.queryStmt.resetExprStack()
	}
}

// EnterWhereClause is called when production whereClause is entered.
func (l *listener) EnterWhereClause(_ *grammar.WhereClauseContext) {
	if l.queryStmt != nil {
		l.queryStmt.resetExprStack()
	}
}

// EnterFieldExpr is called when production fieldExpr is entered.
func (l *listener) EnterFieldExpr(ctx *grammar.FieldExprContext) {
	if l.queryStmt != nil {
		l.queryStmt.visitFieldExpr(ctx)
	}
}

// ExitFieldExpr is called when production fieldExpr is exited.
func (l *listener) ExitFieldExpr(ctx *grammar.FieldExprContext) {
	if l.queryStmt != nil {
		l.queryStmt.completeFieldExpr(ctx)
	}
}

// EnterFuncName is called when production exprFunc is entered.
func (l *listener) EnterFuncName(ctx *grammar.FuncNameContext) {
	if l.queryStmt != nil {
		l.queryStmt.visitFuncName(ctx)
	}
}

// ExitExprFunc is called when production exprFunc is exited.
func (l *listener) ExitExprFunc(_ *grammar.ExprFuncContext) {
	if l.queryStmt != nil {
		l.queryStmt.completeFuncExpr()
	}
}

// EnterExprAtom is called when production exprAtom is entered.
func (l *listener) EnterExprAtom(ctx *grammar.ExprAtomContext) {
	if l.queryStmt != nil {
		l.queryStmt.visitExprAtom(ctx)
	}
}

// EnterAlias is called when production alias is entered.
func (l *listener) EnterAlias(ctx *grammar.AliasContext) {
	if l.queryStmt != nil {
		l.queryStmt.visitAlias(ctx)
	}
}

// EnterLimitClause is called when production limitClause is entered.
func (l *listener) EnterLimitClause(ctx *grammar.LimitClauseContext) {
	switch {
	case l.queryStmt != nil:
		l.queryStmt.visitLimit(ctx)
	case l.metaStmt != nil:
		l.metaStmt.visitLimit(ctx)
	}
}

// EnterTagFilterExpr is called when production tagFilterExpr is entered.
func (l *listener) EnterTagFilterExpr(ctx *grammar.TagFilterExprContext) {
	switch {
	case l.queryStmt != nil:
		l.queryStmt.visitTagFilterExpr(ctx)
	case l.metaStmt != nil:
		l.metaStmt.visitTagFilterExpr(ctx)
	}
}

// ExitTagFilterExpr is called when production tagValueList is exited.
func (l *listener) ExitTagFilterExpr(_ *grammar.TagFilterExprContext) {
	switch {
	case l.queryStmt != nil:
		l.queryStmt.completeTagFilterExpr()
	case l.metaStmt != nil:
		l.metaStmt.completeTagFilterExpr()
	}
}

// EnterTagValue is called when production tagValue is entered.
func (l *listener) EnterTagValue(ctx *grammar.TagValueContext) {
	switch {
	case l.queryStmt != nil:
		l.queryStmt.visitTagValue(ctx)
	case l.metaStmt != nil:
		l.metaStmt.visitTagValue(ctx)
	}
}

// EnterTimeRangeExpr is called when production timeRangeExpr is entered.
func (l *listener) EnterTimeRangeExpr(ctx *grammar.TimeRangeExprContext) {
	if l.queryStmt != nil {
		l.queryStmt.visitTimeRangeExpr(ctx)
	}
}

// EnterGroupByKey is called when production groupByClause is entered.
func (l *listener) EnterGroupByKey(ctx *grammar.GroupByKeyContext) {
	if l.queryStmt != nil {
		l.queryStmt.visitGroupByKey(ctx)
	}
}

// statement returns query statement, if failure return error
func (l *listener) statement() (stmt.Statement, error) {
	switch {
	case l.useStmt != nil:
		return l.useStmt.build()
	case l.storageStmt != nil:
		return l.storageStmt.build()
	case l.schemasStmt != nil:
		return l.schemasStmt.build()
	case l.queryStmt != nil:
		return l.queryStmt.build()
	case l.metaStmt != nil:
		return l.metaStmt.build()
	case l.stateStmt != nil:
		return l.stateStmt.build()
	default:
		return nil, nil
	}
}
