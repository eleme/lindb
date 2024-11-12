package analyzer

import (
	"fmt"

	"github.com/samber/lo"

	"github.com/lindb/lindb/sql/tree"
)

type RelationType string

var (
	TableRelation   RelationType = "Table"
	AliasedRelation RelationType = "Aliased"
	JoinRelation    RelationType = "Join"
	UnknownRelation RelationType = "Unknown"
)

type Relation struct {
	Fields []*tree.Field

	// cannot use field name, field name is empty when select item isn't Identifier/Dereference
	FieldIndexes map[*tree.Field]tree.FieldIndex

	fieldsMap map[tree.FieldIndex]*tree.Field
}

func NewRelation(fields []*tree.Field) *Relation {
	rt := &Relation{
		Fields:       fields,
		FieldIndexes: make(map[*tree.Field]tree.FieldIndex),
		fieldsMap:    make(map[tree.FieldIndex]*tree.Field),
	}
	for _, f := range fields {
		rt.FieldIndexes[f] = f.Index
		rt.fieldsMap[f.Index] = f
	}
	fmt.Printf("new relation fields=%v\n", rt.FieldIndexes)
	return rt
}

func (r *Relation) withAlias(relationAlias string, columnAliases []string) *Relation {
	var fields []*tree.Field
	for i := range r.Fields {
		field := r.Fields[i]
		columnAlias := field.Name
		if len(columnAliases) != 0 {
			columnAlias = columnAliases[i]
		}
		fmt.Printf("columnAlias=%s,columnAliases=%v", columnAlias, columnAliases)

		fields = append(fields, &tree.Field{
			Name:          columnAlias,
			DataType:      field.DataType,
			AggType:       field.AggType,
			Index:         field.Index,
			RelationAlias: tree.NewQualifiedName([]*tree.Identifier{{Value: relationAlias}}),
		})
	}
	return NewRelation(fields)
}

func (r *Relation) joinWith(other *Relation) *Relation {
	var fields []*tree.Field
	fields = append(fields, r.Fields...)
	fields = append(fields, other.Fields...)
	return NewRelation(fields)
}

func (r *Relation) getFieldByIndex(fieldIndex tree.FieldIndex) *tree.Field {
	return r.fieldsMap[fieldIndex]
}

func (r *Relation) resolveFields(name *tree.QualifiedName) (result []*tree.Field) {
	return lo.Filter(r.Fields, func(item *tree.Field, _ int) bool {
		return item.CanResolve(name)
	})
}

func (r *Relation) IndexOf(field *tree.Field) tree.FieldIndex {
	fmt.Printf("relation index of %v\n", r.FieldIndexes)
	return r.FieldIndexes[field]
}

type RelationID struct {
	SourceNode tree.Node
}

func NewRelationID(sourceNode tree.Node) *RelationID {
	return &RelationID{
		SourceNode: sourceNode,
	}
}
