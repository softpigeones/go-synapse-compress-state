// Copyright 2018 New Vector Ltd
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package graphing

import (
	"fmt"
	"os"

	"synapse_compress_state/pkg/compressor"
)

type Graph map[int64]*compressor.StateGroupEntry

// MakeGraphs outputs information from two state group graph into files.
//
// These can be loaded into something like Gephi to visualise the graphs
// before and after the compressor is run.
func MakeGraphs(before, after Graph) {
	// Open all the files to output to
	beforeEdgesFile, err := os.Create("before_edges.csv")
	if err != nil {
		panic(fmt.Sprintf("Error creating before_edges.csv: %v", err))
	}
	defer beforeEdgesFile.Close()

	beforeNodesFile, err := os.Create("before_nodes.csv")
	if err != nil {
		panic(fmt.Sprintf("Error creating before_nodes.csv: %v", err))
	}
	defer beforeNodesFile.Close()

	afterEdgesFile, err := os.Create("after_edges.csv")
	if err != nil {
		panic(fmt.Sprintf("Error creating after_edges.csv: %v", err))
	}
	defer afterEdgesFile.Close()

	afterNodesFile, err := os.Create("after_nodes.csv")
	if err != nil {
		panic(fmt.Sprintf("Error creating after_nodes.csv: %v", err))
	}
	defer afterNodesFile.Close()

	// Write before's information to before_edges and before_nodes
	outputCSV(before, beforeEdgesFile, beforeNodesFile)
	// Write after's information to after_edges and after_nodes
	outputCSV(after, afterEdgesFile, afterNodesFile)
}

// outputCSV outputs information from a state group graph into an edges file and a node file.
//
// These can be loaded into something like Gephi to visualise the graphs.
func outputCSV(groups Graph, edgesOutput, nodesOutput *os.File) {
	// The line A;B in the edges file means:
	//      That state group A has predecessor B
	fmt.Fprintln(edgesOutput, "Source;Target")

	// The line A;B;C;"B" in the nodes file means:
	//      The state group id is A
	//      This state group has B rows in the state_groups_state table
	//      If C is true then A has no predecessor
	fmt.Fprintln(nodesOutput, "Id;Rows;Root;Label")

	for source, entry := range groups {
		// If the group has a predecessor then write an edge in the edges file
		if entry.PrevStateGroup != nil {
			fmt.Fprintf(edgesOutput, "%d;%d\n", source, *entry.PrevStateGroup)
		}

		// Write the state group's information to the nodes file
		var root bool
		if entry.PrevStateGroup == nil {
			root = true
		}
		fmt.Fprintf(nodesOutput, "%d;%d;%t;\"%d\"\n", source, entry.StateMap.Len(), root, entry.StateMap.Len())
	}
}
