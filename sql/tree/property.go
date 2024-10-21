package tree

type Property struct {
	BaseNode

	Name  *Identifier
	Value Expression
}

func (n *Property) Accept(context any, vistor Visitor) any {
	return vistor.Visit(context, n)
}
