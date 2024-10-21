package execution

import (
	"github.com/lindb/lindb/meta"
	"github.com/lindb/lindb/models"
	"github.com/lindb/lindb/sql/analyzer"
)

type Deps struct {
	MetaMgr     meta.MetadataManager
	CurrentNode *models.InternalNode
	AnalyzerFct *analyzer.AnalyzerFactory
}
