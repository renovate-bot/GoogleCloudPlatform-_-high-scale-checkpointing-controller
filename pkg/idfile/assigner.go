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
	"slices"
	"strings"

	"k8s.io/klog/v2"
)

// assigner extends a rank assignment to all given nodes.
type assigner struct {
	numWorkers int
	sliceSize  int
	numSlices  int

	pools map[string]bool
	nodes map[string]*assignerNode
}

type assignerNode struct {
	name        string
	pool        string
	currentRank int
	initialRank int
}

func newAssigner(numSlices, sliceSize int) *assigner {
	numWorkers := numSlices * sliceSize
	return &assigner{
		numWorkers: numWorkers,
		sliceSize:  sliceSize,
		numSlices:  numSlices,
		pools:      map[string]bool{},
		nodes:      map[string]*assignerNode{},
	}
}

func (a *assigner) dump() string {
	nodes := []string{}
	for n := range a.nodes {
		nodes = append(nodes, n)
	}
	slices.Sort(nodes)

	strs := []string{}
	for _, n := range nodes {
		nn := a.nodes[n]
		strs = append(strs, fmt.Sprintf("%s/%s %d (%d)", nn.name, nn.pool, nn.currentRank, nn.initialRank))
	}
	return strings.Join(strs, "\n")
}

func (a *assigner) addNode(name, pool string, currentRank, initialRank int) error {
	if currentRank >= a.numWorkers {
		klog.Warningf("Node %s current rank out of range: %d out of %d. Resetting", name, currentRank, a.numWorkers)
		currentRank = -1
	}
	if initialRank >= a.numWorkers {
		klog.Warningf("Node %s initial rank out of range: %d out of %d. Resetting", name, initialRank, a.numWorkers)
		initialRank = -1
	}
	a.pools[pool] = true
	node := &assignerNode{
		name:        name,
		pool:        pool,
		currentRank: currentRank,
		initialRank: initialRank,
	}
	a.nodes[node.name] = node
	return nil
}

func (a *assigner) clearCurrentRanks() {
	for _, node := range a.nodes {
		node.currentRank = -1
	}
}

func (a *assigner) supersliceAssignment() ([]string, error) {
	// Superslicing uses cubes of 16 VMs that map to node pools. For testing, smaller cubes may be used.
	// The ranks server figures out if we're superslicing or not, so we'll compute the cube size from
	// the observed topology.
	if len(a.pools) == 0 {
		return nil, fmt.Errorf("no pools detected in current assignment")
	}
	cubeSize := a.numWorkers / len(a.pools)
	if a.numWorkers%cubeSize != 0 {
		return nil, fmt.Errorf("%d workers do not evenly divide detected cube size of %d", a.numWorkers, cubeSize)
	}

	orderedPools, err := a.superslicePoolOrderGuess(cubeSize)
	if err != nil {
		klog.Warningf("Can't find ordered pools, will assign arbitrarily: %v", err)
		orderedPools = nil
		for p := range a.pools {
			orderedPools = append(orderedPools, p)
		}
	}

	var rankAssignment []string
	for k, p := range orderedPools {
		nodes, err := a.assignSuperslicePoolIndex(p, k, cubeSize)
		if err != nil {
			return nil, fmt.Errorf("Bad node list: %v", err)
		}
		rankAssignment = append(rankAssignment, nodes...)
	}
	return rankAssignment, nil
}

// assignSuperslicePoolIndex returns the nodes falling in a pool, assigning them a rank of
// cubeSize*k+i for some 0 <= i < cubeSize. Any node whose rank is already in that range is
// unchanged, otherwise it's assigned arbitrarily. The node names in that rank order are returned.
func (a *assigner) assignSuperslicePoolIndex(pool string, k, cubeSize int) ([]string, error) {
	rankedNodes := make([]string, cubeSize)
	unassigned := []string{}
	var nodes []string
	for name, node := range a.nodes {
		if node.pool == pool {
			nodes = append(nodes, name)
		}
	}
	slices.Sort(nodes) // Make order deterministic, mostly for testing.
	for _, name := range nodes {
		node := a.nodes[name]
		r := node.currentRank
		if r < 0 {
			r = node.initialRank
		}
		idx := r % cubeSize
		if r >= k*cubeSize && r < (k+1)*cubeSize && rankedNodes[idx] == "" {
			rankedNodes[idx] = name
		} else {
			unassigned = append(unassigned, name)
		}
	}
	for _, node := range unassigned {
		var i int
		for i = 0; i < cubeSize; i++ {
			if rankedNodes[i] == "" {
				break
			}
		}
		if i >= cubeSize {
			return nil, fmt.Errorf("Too many nodes in pool %s", pool)
		}
		rankedNodes[i] = node
	}
	for i := 0; i < cubeSize; i++ {
		if rankedNodes[i] == "" {
			return nil, fmt.Errorf("Not enough nodes in pool %s", pool)
		}
	}
	return rankedNodes, nil
}

// superslicePoolOrderGuess gives a first guess as to which workers should be in a pool.
// We'll use the lowest index appearing in a pool, rounded down to a multiple of cubeSize.
// The return value is a list of the pools in that order of indices, choosing arbitrarily
// when there's a conflict.
func (a *assigner) superslicePoolOrderGuess(cubeSize int) ([]string, error) {
	poolIndicies := map[string]int{}
	for _, node := range a.nodes {
		r := node.currentRank
		if r < 0 {
			r = node.initialRank
		}
		if r < 0 {
			continue
		}
		curr, found := poolIndicies[node.pool]
		if !found || r < curr {
			poolIndicies[node.pool] = r
		}
	}
	var pools []string
	for p, i := range poolIndicies {
		poolIndicies[p] = i - (i % cubeSize)
		pools = append(pools, p)
	}
	// Use a deterministic order, mostly for testing.
	slices.Sort(pools)

	existingStarts := map[int]string{}
	for _, p := range pools {
		i := poolIndicies[p]
		if _, found := existingStarts[i]; found {
			delete(poolIndicies, p)
		} else {
			existingStarts[i] = p
		}
	}
	var unassignedPools []string
	for p := range a.pools {
		if _, found := poolIndicies[p]; !found {
			unassignedPools = append(unassignedPools, p)
		}
	}
	// Make the unassigned pools deterministic, mostly for testing.
	slices.Sort(unassignedPools)

	orderedPools := make([]string, len(a.pools))
	freePool := 0
	for i := 0; i < len(a.pools); i++ {
		if p, found := existingStarts[i*cubeSize]; found {
			orderedPools[i] = p
		} else {
			if freePool >= len(unassignedPools) {
				return nil, fmt.Errorf("Unexpected pool start mismatch")
			}
			orderedPools[i] = unassignedPools[freePool]
			freePool++
		}
	}
	return orderedPools, nil
}

func (a *assigner) extendFromInitialRanks() ([]string, error) {
	rankAssignment := make([]*assignerNode, a.numWorkers)
	for _, node := range a.nodes {
		if node.currentRank >= 0 && node.initialRank >= 0 && node.currentRank != node.initialRank {
			return nil, fmt.Errorf("inconsistent initial ranks")
		}
		if node.currentRank >= 0 {
			rankAssignment[node.currentRank] = node
		} else if node.initialRank >= 0 {
			rankAssignment[node.initialRank] = node
		}
	}
	return a.verifyAssignment(rankAssignment)
}

func (a *assigner) extendFromCurrentRank() ([]string, error) {
	slicePool := make([]string, a.numSlices)
	unusedPools := map[string]bool{}
	for _, node := range a.nodes {
		unusedPools[node.pool] = true
	}
	availableInPool := make(map[string][]*assignerNode)
	rankAssignment := make([]*assignerNode, a.numWorkers)
	for _, node := range a.nodes {
		if node.currentRank >= 0 {
			slice := node.currentRank / a.sliceSize
			if slicePool[slice] != "" && slicePool[slice] != node.pool {
				return nil, fmt.Errorf("inconsistent pools for current rank %d/%d/%d node %s %s vs %s", slice, node.currentRank%a.sliceSize, node.currentRank, node.name, slicePool[slice], node.pool)
			}
			slicePool[slice] = node.pool
			delete(unusedPools, node.pool)
			rankAssignment[node.currentRank] = node
		} else {
			availableInPool[node.pool] = append(availableInPool[node.pool], node)
		}
	}

	for i := 0; i < a.numWorkers; i++ {
		if rankAssignment[i] != nil {
			continue
		}
		slice := i / a.sliceSize
		if slicePool[slice] == "" {
			var pool string
			for pool = range unusedPools {
				break
			}
			if pool == "" {
				return nil, fmt.Errorf("exhausted pools")
			}
			slicePool[slice] = pool
			delete(unusedPools, pool)
		}
		pool := slicePool[slice]
		num := len(availableInPool[pool])
		if num == 0 {
			return nil, fmt.Errorf("exhausted pool %s", pool)
		}
		node := availableInPool[pool][num-1]
		availableInPool[pool] = availableInPool[pool][:num-1]
		rankAssignment[i] = node
	}

	return a.verifyAssignment(rankAssignment)
}

func (a *assigner) forceArbitraryAssignment() ([]string, error) {
	nodes := []string{}
	for _, node := range a.nodes {
		nodes = append(nodes, node.name)
	}
	// Sort by name for deterministic assignment
	slices.Sort(nodes)

	if len(nodes) != a.numWorkers {
		return nil, fmt.Errorf("incorrect node count for arbitrary assignment: %d != %d", len(nodes), a.numWorkers)
	}

	// We don't verify assignment here because it violates pool/slice constraints by definition
	return nodes, nil
}

func (a *assigner) existingAssignment() []string {
	if len(a.nodes) != a.numWorkers {
		return nil
	}
	rankAssignment := make([]*assignerNode, a.numWorkers)
	for _, node := range a.nodes {
		if node.currentRank >= 0 && node.currentRank < a.numWorkers {
			rankAssignment[node.currentRank] = node
		}
	}
	if rankedNodes, err := a.verifyAssignment(rankAssignment); err == nil {
		return rankedNodes
	}
	return nil
}

func (a *assigner) verifyAssignment(rankAssignment []*assignerNode) ([]string, error) {
	mkerr := func(msg string, args ...any) error {
		nodeStr := []string{}
		for _, n := range rankAssignment {
			if n != nil {
				nodeStr = append(nodeStr, fmt.Sprintf("%+v", *n))
			} else {
				nodeStr = append(nodeStr, "<nil>")
			}
		}
		return fmt.Errorf("%s: %s", fmt.Sprintf(msg, args...), strings.Join(nodeStr, " "))
	}

	if len(rankAssignment) != a.numWorkers {
		return nil, mkerr("incomplete assignment (%d / %d)", len(rankAssignment), a.numWorkers)
	}

	slicePool := make([]string, a.numWorkers)
	for i, node := range rankAssignment {
		if node == nil {
			return nil, mkerr("no assignment for %d", i)
		}
		slice := i / a.sliceSize
		if slicePool[slice] == "" {
			slicePool[slice] = node.pool
		} else if node.pool != slicePool[slice] {
			return nil, mkerr("pool mismatch at %d: %s vs %s", i, node.pool, slicePool[slice])
		}
	}

	// Success!
	rankedNodes := make([]string, a.numWorkers)
	for i, n := range rankAssignment {
		rankedNodes[i] = n.name
	}
	return rankedNodes, nil
}
