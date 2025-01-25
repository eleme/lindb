package flow

type Downsampling interface{}

type Stream interface {
	SetAtStep(step int, value float64, fn func(a, b float64) float64)
	GetAtStep(step int) float64
	Merge(stream Stream, fn func(a, b float64) float64)
	Values() []float64
}
