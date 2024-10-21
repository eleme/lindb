package tree

type DropDatabase struct {
	BaseNode
	Name   string
	Exists bool
}

func (n *DropDatabase) Accept(context any, visitor Visitor) (r any) {
	return visitor.Visit(context, n)
}
