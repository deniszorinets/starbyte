package sdk

type Step interface {
	Transform(any) (any, error)
}

type StepBefore interface {
	BeforeTransform() error
}

type StepAfter interface {
	AfterTransform() error
}
