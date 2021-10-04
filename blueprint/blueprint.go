// Copyright 2021 Dolthub, Inc.
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

package blueprint

import "time"

// Blueprint is used by hooked functions to describe the exact plans of this run. A Hook may modify any of the values here.
type Blueprint struct {
	// CycleStart is the time that the cycle started at.
	CycleStart time.Time
	// BranchCount is the number of branches that will be created for this repository.
	BranchCount uint64
	// TableCount is the number of tables that will be created on the main branch for this repository.
	TableCount uint64
	// TargetRowCount is the number of rows that each table of each branch is targeting. The first map is the branch name,
	// while the second map is the table name.
	TargetRowCount map[string]map[string]uint64
	// SQLStatementsExecuted is the total number of SQL statements executed.
	SQLStatementsExecuted uint64
}
