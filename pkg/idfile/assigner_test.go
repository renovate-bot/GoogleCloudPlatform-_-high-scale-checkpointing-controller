// Copyright 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package idfile

import (
	"fmt"
	"reflect"
	"testing"

	"gotest.tools/v3/assert"
)

func TestAssignerInitial3x2(t *testing.T) {
	a := newAssigner(3, 2)
	assert.Assert(t, a != nil)
	a.addNode("n0", "p0", -1, 0)
	a.addNode("n1", "p0", -1, 1)
	a.addNode("n2", "p1", -1, 2)
	a.addNode("n3", "p1", -1, 3)
	a.addNode("n4", "p2", -1, 4)
	a.addNode("n5", "p2", -1, 5)

	asg, err := a.extendFromInitialRanks()
	assert.NilError(t, err)

	assert.Equal(t, asg[0], "n0")
	assert.Equal(t, asg[1], "n1")
	assert.Equal(t, asg[2], "n2")
	assert.Equal(t, asg[3], "n3")
	assert.Equal(t, asg[4], "n4")
	assert.Equal(t, asg[5], "n5")
}

func TestAssignerExtend3x2(t *testing.T) {
	a := newAssigner(3, 2)
	assert.Assert(t, a != nil)
	a.addNode("n0", "p0", -1, 0)
	a.addNode("n1", "p0", -1, 1)
	a.addNode("n2", "p1", 2, 2)
	a.addNode("n3", "p1", -1, 3)
	a.addNode("n4", "p2", 4, 4)
	a.addNode("n5", "p2", -1, 5)

	asg, err := a.extendFromInitialRanks()
	assert.NilError(t, err)

	assert.Equal(t, asg[0], "n0")
	assert.Equal(t, asg[1], "n1")
	assert.Equal(t, asg[2], "n2")
	assert.Equal(t, asg[3], "n3")
	assert.Equal(t, asg[4], "n4")
	assert.Equal(t, asg[5], "n5")
}

func TestAssignerExtendWithSwap3x2(t *testing.T) {
	a := newAssigner(3, 2)
	assert.Assert(t, a != nil)
	a.addNode("n0", "p0", 1, 0)
	a.addNode("n1", "p0", -1, 1)
	a.addNode("n2", "p1", -1, 2)
	a.addNode("n3", "p1", -1, 3)
	a.addNode("n4", "p2", -1, 4)
	a.addNode("n5", "p2", -1, 5)

	_, err := a.extendFromInitialRanks()
	assert.ErrorContains(t, err, "inconsistent")

	_, err = a.extendFromCurrentRank()
	assert.NilError(t, err)
	// The assignment depends on hash ordering so we rely on the verification.
}

func TestAssignerOneFailureFromInitial3x2(t *testing.T) {
	a := newAssigner(3, 2)
	assert.Assert(t, a != nil)
	a.addNode("n0", "p0", 0, 0)
	a.addNode("n1", "p0", 1, 1)
	a.addNode("n2", "p1", 2, 2)
	a.addNode("n3", "p1", -1, 3)
	a.addNode("n4", "p2", 4, 4)
	a.addNode("n5", "p2", 5, 5)

	_, err := a.extendFromInitialRanks()
	assert.NilError(t, err)
}

func TestAssignerOneFailureBadInitial3x2(t *testing.T) {
	a := newAssigner(3, 2)
	assert.Assert(t, a != nil)
	a.addNode("n0", "p0", 0, 1)
	a.addNode("n1", "p0", 1, 2)
	a.addNode("n2", "p1", 2, 4)
	a.addNode("n3", "p1", -1, 3)
	a.addNode("n4", "p2", 4, 5)
	a.addNode("n5", "p2", 5, 0)

	_, err := a.extendFromInitialRanks()
	assert.ErrorContains(t, err, "inconsistent")

	_, err = a.extendFromCurrentRank()
	assert.NilError(t, err)
}

func runManyInitialTest(t *testing.T, slices, sizes []int) {
	runner := func(t *testing.T, numSlices, sliceSize int) {
		t.Parallel()
		a := newAssigner(numSlices, sliceSize)
		for s := 0; s < numSlices; s++ {
			for k := 0; k < sliceSize; k++ {
				i := s*sliceSize + k
				a.addNode(fmt.Sprintf("n%04d", i), fmt.Sprintf("p%04d", s), -1, i)
			}
		}
		asg, err := a.extendFromInitialRanks()
		assert.NilError(t, err)
		for s := 0; s < numSlices; s++ {
			for k := 0; k < sliceSize; k++ {
				i := s*sliceSize + k
				node := fmt.Sprintf("n%04d", i)
				assert.Equal(t, asg[i], node, node)
			}
		}
	}

	for _, numSlices := range slices {
		for _, sliceSize := range sizes {
			i := numSlices
			j := sliceSize
			t.Run(fmt.Sprintf("%d-%d", numSlices, sliceSize), func(t *testing.T) { runner(t, i, j) })
		}
	}
}

func TestAssignerManySmallInitial(t *testing.T) {
	runManyInitialTest(t, []int{1, 2, 3, 4, 5, 6, 10}, []int{1, 2, 3, 4, 8, 16})
}

func TestAssignerManyLargeInitial(t *testing.T) {
	// Don't test beyond 1M nodes, which is already unrealistic.
	runManyInitialTest(t, []int{128, 256, 768, 1024}, []int{32, 128, 512, 1024})
}

func TestAssignerManyUnbalancedInitial(t *testing.T) {
	runManyInitialTest(t, []int{128, 256, 512, 1024, 2048}, []int{1, 2, 8, 16})
	runManyInitialTest(t, []int{1, 2, 4, 8, 16}, []int{128, 256, 512, 1024, 2048})
}

func runManyCurrentTest(t *testing.T, slices, sizes []int) {
	runner := func(t *testing.T, numSlices, sliceSize, numCurrent int) {
		t.Parallel()
		a := newAssigner(numSlices, sliceSize)
		numJobs := numSlices * sliceSize
		for s := 0; s < numSlices; s++ {
			for k := 0; k < sliceSize; k++ {
				i := s*sliceSize + k
				rank := -1
				if numCurrent > 0 && (i+1)%numCurrent == 0 {
					rank = i
				}
				// Note the initial index is forced to not match any currentRank, and if the number
				// of slices & slice size is greater than one, will have an invalid pool assignment.
				a.addNode(fmt.Sprintf("n%04d", i), fmt.Sprintf("p%04d", s), rank, (i+1)%numJobs)
			}
		}
		_, err := a.extendFromInitialRanks()
		if numCurrent > 0 || (numSlices > 1 && sliceSize > 1) {
			if !(numCurrent == 1 && numSlices == 1 && sliceSize == 1) {
				assert.Assert(t, err != nil)
			}
		}
		_, err = a.extendFromCurrentRank()
		assert.NilError(t, err)
	}

	for _, numSlices := range slices {
		for _, sliceSize := range sizes {
			n := numSlices * sliceSize
			used := map[int]bool{}
			for _, numCurrent := range []int{0, 1, n / 4, n / 2, 3 * n / 4, n - 5, n - 2, n - 1} {
				if numCurrent < 0 {
					continue
				}
				if _, found := used[numCurrent]; found {
					continue
				}
				used[numCurrent] = true
				i := numSlices
				j := sliceSize
				k := numCurrent
				t.Run(fmt.Sprintf("%d-%d-%d", i, j, k), func(t *testing.T) { runner(t, i, j, k) })
			}
		}
	}
}

func TestAssignerManySmallExtend(t *testing.T) {
	runManyCurrentTest(t, []int{1, 2, 3, 4, 5, 6, 10}, []int{1, 2, 3, 4, 8, 16})
}

func TestAssignerManyLargeExtend(t *testing.T) {
	// Don't test beyond 1M nodes, which is already unrealistic.
	runManyCurrentTest(t, []int{128, 256, 768, 1024}, []int{32, 128, 512, 1024})
}

func TestAssignerManyUnbalancedExtend(t *testing.T) {
	runManyCurrentTest(t, []int{128, 256, 512, 1024, 2048}, []int{1, 2, 8, 16})
	runManyCurrentTest(t, []int{1, 2, 4, 8, 16}, []int{128, 256, 512, 1024, 2048})
}

func TestAssignerSuperslicePoolOrderGuess(t *testing.T) {
	testCases := []struct {
		name     string
		cubeSize int
		numPools int
		nodes    []*assignerNode
		want     []string
		wantErr  bool
	}{
		{
			name:     "single cube",
			cubeSize: 5,
			numPools: 1,
			nodes: []*assignerNode{
				{pool: "0", currentRank: 3},
			},
			want: []string{"0"},
		},
		{
			name:     "single cube, out of bounds",
			cubeSize: 5,
			numPools: 1,
			nodes: []*assignerNode{
				{pool: "0", currentRank: 5},
			},
			wantErr: true,
		},
		{
			name:     "conflict resolution",
			cubeSize: 10,
			numPools: 3,
			nodes: []*assignerNode{
				{pool: "0", currentRank: 12}, // Rounds to 10
				{pool: "1", currentRank: 15}, // Rounds to 10 -> Conflict!
				{pool: "2", currentRank: 25}, // Rounds to 20
			},
			want: []string{"0", "1", "2"},
		},
		{
			name:     "rank fallback and gaps",
			cubeSize: 2,
			numPools: 2,
			nodes: []*assignerNode{
				{pool: "0", currentRank: -1, initialRank: 4}, // Rounds to 4 (Index 2)
			},
			wantErr: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			pools := map[string]bool{}
			for i := 0; i < tc.numPools; i++ {
				pools[fmt.Sprintf("%d", i)] = true
			}
			nodes := map[string]*assignerNode{}
			for _, n := range tc.nodes {
				nodes[n.name] = n
			}
			a := &assigner{
				nodes: nodes,
				pools: pools,
			}

			got, err := a.superslicePoolOrderGuess(tc.cubeSize)

			if (err != nil) != tc.wantErr {
				t.Errorf("wantErr = %v, got error: %v", tc.wantErr, err)
			}

			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("got %v, want %v", got, tc.want)
			}
		})
	}
}

func TestAssignerSuperslicePoolIndex(t *testing.T) {
	testCases := []struct {
		name     string
		k        int
		cubeSize int
		ranks    []int
		want     []string
		wantErr  bool
	}{
		{
			name:     "preassigned 4 cube",
			k:        0,
			cubeSize: 4,
			ranks:    []int{0, 3, 1, 2},
			want:     []string{"00", "02", "03", "01"},
		},
		{
			name:     "another preassigned 4 cube",
			k:        3,
			cubeSize: 4,
			ranks:    []int{13, 12, 15, 14},
			want:     []string{"01", "00", "03", "02"},
		},
		{
			name:     "4 cube out of range",
			k:        1,
			cubeSize: 4,
			ranks:    []int{5, 7, 4, 1},
			want:     []string{"02", "00", "03", "01"},
		},
		{
			name:     "4 cube duplicate",
			k:        0,
			cubeSize: 4,
			ranks:    []int{0, 1, 3, 1},
			want:     []string{"00", "01", "03", "02"},
		},
		{
			name:     "16 cube nearly there",
			k:        1,
			cubeSize: 16,
			ranks:    []int{16, 17, 18, 19, 25, 21, 22, 23, 24, 20, 26, 27, 28, 29, 30, 31},
			want:     []string{"00", "01", "02", "03", "09", "05", "06", "07", "08", "04", "10", "11", "12", "13", "14", "15"},
		},
		{
			name:     "too many nodes",
			k:        0,
			cubeSize: 4,
			ranks:    []int{0, 1, 2, 3, 4},
			wantErr:  true,
		},
		{
			name:     "missing nodes",
			k:        0,
			cubeSize: 4,
			ranks:    []int{0, 1, 2},
			wantErr:  true,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			a := newAssigner(tc.cubeSize, tc.k+1)
			for i, r := range tc.ranks {
				if r&1 == 1 {
					a.addNode(fmt.Sprintf("%02d", i), "pool", -1, r)
				} else {
					a.addNode(fmt.Sprintf("%02d", i), "pool", r, r+1)
				}
			}
			// A couple random nodes which should be ignored.
			a.addNode("random-0", "another-pool", 0, -1)
			a.addNode("random-1", "another-pool", -1, 0)
			rankedNodes, err := a.assignSuperslicePoolIndex("pool", tc.k, tc.cubeSize)
			if (err != nil) != tc.wantErr {
				t.Errorf("wantErr = %v, got error: %v", tc.wantErr, err)
			}
			if !reflect.DeepEqual(rankedNodes, tc.want) {
				t.Errorf("got %v, want %v", rankedNodes, tc.want)
			}
		})
	}
}

func TestAssignerSuperslice(t *testing.T) {
	testCases := []struct {
		name                        string
		slices, sliceSize, cubeSize int
		nodes                       []int
		want                        []string
		wantErr                     bool
	}{
		{
			name:      "2x4, already ordered",
			slices:    1,
			sliceSize: 8,
			cubeSize:  4,
			nodes:     []int{0, 1, 2, 3, 4, 5, 6, 7},
			want:      []string{"00", "01", "02", "03", "04", "05", "06", "07"},
		},
		{
			name:      "2x4, deranged",
			slices:    1,
			sliceSize: 8,
			cubeSize:  4,
			nodes:     []int{0, 1, 4, 3, 2, 5, 6, 7},
			want:      []string{"00", "01", "02", "03", "04", "05", "06", "07"},
		},
		{
			name:      "2x4, rearranged",
			slices:    1,
			sliceSize: 8,
			cubeSize:  4,
			nodes:     []int{0, 1, 3, 4, 2, 5, 6, 7},
			want:      []string{"00", "01", "03", "02", "04", "05", "06", "07"},
		},
		{
			name:      "2x4, mixed up within cube",
			slices:    1,
			sliceSize: 8,
			cubeSize:  4,
			nodes:     []int{3, 1, 2, 0, 7, 6, 4, 5},
			want:      []string{"03", "01", "02", "00", "06", "07", "05", "04"},
		},
		{
			name:      "single node pool",
			slices:    1,
			sliceSize: 4,
			cubeSize:  4,
			nodes:     []int{0, 3, 2, 1},
			want:      []string{"00", "03", "02", "01"},
		},
		{
			name:      "multislice",
			slices:    3,
			sliceSize: 32,
			cubeSize:  16,
			wantErr:   true,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			a := newAssigner(tc.slices, tc.sliceSize)
			for i, r := range tc.nodes {
				poolStr := fmt.Sprintf("p%02d", i/tc.cubeSize)
				nodeStr := fmt.Sprintf("%02d", i)
				if r&1 == 1 {
					a.addNode(nodeStr, poolStr, -1, r)
				} else {
					a.addNode(nodeStr, poolStr, r, r+1)
				}
			}
			rankedNodes, err := a.supersliceAssignment()
			if (err != nil) != tc.wantErr {
				t.Errorf("wantErr = %v, got error: %v", tc.wantErr, err)
			}
			if !reflect.DeepEqual(rankedNodes, tc.want) {
				t.Errorf("got %v, want %v", rankedNodes, tc.want)
			}
		})
	}
}
