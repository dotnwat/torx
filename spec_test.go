package torx

import (
	"reflect"
	"testing"
)

func TestOptional(t *testing.T) {
	if v, ok := Some(4.0).Get(); !ok || v != 4.0 {
		t.Errorf("Some(4.0).Get() = (%v, %v), want (4, true)", v, ok)
	}
	var zero Optional[int]
	if v, ok := zero.Get(); ok || v != 0 {
		t.Errorf("zero Optional[int].Get() = (%v, %v), want (0, false)", v, ok)
	}
}

func TestLabelsSubset(t *testing.T) {
	tests := []struct {
		name string
		l    Labels
		of   Labels
		want bool
	}{
		{"empty subset of empty", NewLabels(), NewLabels(), true},
		{"empty subset of any", NewLabels(), NewLabels("a"), true},
		{"nil subset of any", nil, NewLabels("a"), true},
		{"subset", NewLabels("a", "b"), NewLabels("a", "b", "c"), true},
		{"equal", NewLabels("a"), NewLabels("a"), true},
		{"missing one", NewLabels("a", "b"), NewLabels("a"), false},
		{"missing all", NewLabels("a"), nil, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.l.Subset(tc.of); got != tc.want {
				t.Errorf("Subset = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestNodeSpecSatisfiedBy(t *testing.T) {
	tests := []struct {
		name     string
		required Resources
		capacity Resources
		want     bool
	}{
		{
			name:     "no requirements met by empty capacity",
			required: Resources{},
			capacity: Resources{},
			want:     true,
		},
		{
			name:     "cpus met",
			required: Resources{CPUs: Some(2.0)},
			capacity: Resources{CPUs: Some(4.0)},
			want:     true,
		},
		{
			name:     "cpus exact",
			required: Resources{CPUs: Some(2.0)},
			capacity: Resources{CPUs: Some(2.0)},
			want:     true,
		},
		{
			name:     "cpus insufficient",
			required: Resources{CPUs: Some(4.0)},
			capacity: Resources{CPUs: Some(2.0)},
			want:     false,
		},
		{
			name:     "cpus required but capacity unknown",
			required: Resources{CPUs: Some(1.0)},
			capacity: Resources{},
			want:     false,
		},
		{
			name:     "memory met",
			required: Resources{MemoryMB: Some(1024)},
			capacity: Resources{MemoryMB: Some(2048)},
			want:     true,
		},
		{
			name:     "memory insufficient",
			required: Resources{MemoryMB: Some(2048)},
			capacity: Resources{MemoryMB: Some(1024)},
			want:     false,
		},
		{
			name:     "labels met",
			required: Resources{Labels: NewLabels("nvme")},
			capacity: Resources{Labels: NewLabels("nvme", "gpu")},
			want:     true,
		},
		{
			name:     "labels missing",
			required: Resources{Labels: NewLabels("nvme")},
			capacity: Resources{Labels: NewLabels("gpu")},
			want:     false,
		},
		{
			name: "all dimensions met",
			required: Resources{
				CPUs:     Some(2.0),
				MemoryMB: Some(1024),
				Labels:   NewLabels("nvme"),
			},
			capacity: Resources{
				CPUs:     Some(8.0),
				MemoryMB: Some(4096),
				Labels:   NewLabels("nvme", "bare-metal"),
			},
			want: true,
		},
		{
			name: "one dimension short fails the whole spec",
			required: Resources{
				CPUs:     Some(2.0),
				MemoryMB: Some(8192),
				Labels:   NewLabels("nvme"),
			},
			capacity: Resources{
				CPUs:     Some(8.0),
				MemoryMB: Some(4096),
				Labels:   NewLabels("nvme"),
			},
			want: false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			spec := NodeSpec{Required: tc.required}
			if got := spec.SatisfiedBy(tc.capacity); got != tc.want {
				t.Errorf("SatisfiedBy = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestHomogeneous(t *testing.T) {
	spec := NodeSpec{
		OS:       Linux,
		Required: Resources{CPUs: Some(2.0), Labels: NewLabels("nvme")},
		Role:     "broker",
	}
	p := Homogeneous(3, spec)
	if p.Size() != 3 {
		t.Fatalf("Size() = %d, want 3", p.Size())
	}
	for i, n := range p.Nodes {
		if !reflect.DeepEqual(n, spec) {
			t.Errorf("node %d = %+v, want %+v", i, n, spec)
		}
	}
}

func TestHomogeneousNonPositive(t *testing.T) {
	if got := Homogeneous(0, NodeSpec{}).Size(); got != 0 {
		t.Errorf("Homogeneous(0).Size() = %d, want 0", got)
	}
	if got := Homogeneous(-1, NodeSpec{}).Size(); got != 0 {
		t.Errorf("Homogeneous(-1).Size() = %d, want 0", got)
	}
}
