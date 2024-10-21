package analyzer

import (
	"github.com/lindb/lindb/sql/tree"
)

type FieldID struct {
	RelationID *RelationID
	FieldIndex tree.FieldIndex
}

type ResolvedField struct {
	Scope               *Scope
	Field               *tree.Field
	HierarchyFieldIndex tree.FieldIndex
	RelationFieldIndex  tree.FieldIndex
	Local               bool
}

func (rf *ResolvedField) FieldID() *FieldID {
	return &FieldID{
		RelationID: rf.Scope.RelationID,
		FieldIndex: rf.Scope.RelationType.IndexOf(rf.Field),
	}
}
