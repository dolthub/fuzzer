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

package commands

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/fuzzer/run"
	"github.com/dolthub/fuzzer/types"
)

var baseRows = []run.Row{
	{[]types.Value{types.TimeValue{"-838:31:51"}, types.TimeValue{"-553:00:01"}}, 1},
	{[]types.Value{types.TimeValue{"-793:52:40"}, types.TimeValue{"198:43:35"}}, 1},
	{[]types.Value{types.TimeValue{"-793:26:03"}, types.TimeValue{"715:26:13"}}, 1},
	{[]types.Value{types.TimeValue{"-764:41:36"}, types.TimeValue{"-453:16:19"}}, 1},
	{[]types.Value{types.TimeValue{"-690:22:04"}, types.TimeValue{"163:36:57"}}, 1},
	{[]types.Value{types.TimeValue{"-684:57:11"}, types.TimeValue{"-284:39:55"}}, 1},
	{[]types.Value{types.TimeValue{"-656:59:25"}, types.TimeValue{"394:55:53"}}, 1},
	{[]types.Value{types.TimeValue{"-650:19:06"}, types.TimeValue{"-177:56:40"}}, 1},
	{[]types.Value{types.TimeValue{"-589:39:43"}, types.TimeValue{"-434:53:33"}}, 1},
	{[]types.Value{types.TimeValue{"-442:15:39"}, types.TimeValue{"758:18:25"}}, 1},
	{[]types.Value{types.TimeValue{"-308:43:24"}, types.TimeValue{"178:04:43"}}, 1},
	{[]types.Value{types.TimeValue{"-203:32:21"}, types.TimeValue{"-330:29:49"}}, 1},
	{[]types.Value{types.TimeValue{"-178:55:18"}, types.TimeValue{"636:15:55"}}, 1},
	{[]types.Value{types.TimeValue{"18:06:59"}, types.TimeValue{"-12:57:13"}}, 1},
	{[]types.Value{types.TimeValue{"37:22:18"}, types.TimeValue{"-699:29:43"}}, 1},
	{[]types.Value{types.TimeValue{"67:35:37"}, types.TimeValue{"347:22:17"}}, 1},
	{[]types.Value{types.TimeValue{"124:04:30"}, types.TimeValue{"507:39:02"}}, 1},
	{[]types.Value{types.TimeValue{"135:41:08"}, types.TimeValue{"-812:36:16"}}, 1},
	{[]types.Value{types.TimeValue{"135:48:02"}, types.TimeValue{"-166:38:15"}}, 1},
	{[]types.Value{types.TimeValue{"170:50:26"}, types.TimeValue{"465:34:49"}}, 1},
	{[]types.Value{types.TimeValue{"220:24:44"}, types.TimeValue{"-607:19:42"}}, 1},
	{[]types.Value{types.TimeValue{"272:18:02"}, types.TimeValue{"-450:21:45"}}, 1},
	{[]types.Value{types.TimeValue{"286:30:14"}, types.TimeValue{"393:52:48"}}, 1},
	{[]types.Value{types.TimeValue{"327:06:35"}, types.TimeValue{"-830:54:12"}}, 1},
	{[]types.Value{types.TimeValue{"349:06:13"}, types.TimeValue{"-333:14:49"}}, 1},
	{[]types.Value{types.TimeValue{"390:38:57"}, types.TimeValue{"11:05:46"}}, 1},
	{[]types.Value{types.TimeValue{"390:57:07"}, types.TimeValue{"-689:08:04"}}, 1},
	{[]types.Value{types.TimeValue{"461:43:15"}, types.TimeValue{"-333:06:18"}}, 1},
	{[]types.Value{types.TimeValue{"490:08:46"}, types.TimeValue{"426:23:30"}}, 1},
	{[]types.Value{types.TimeValue{"521:45:58"}, types.TimeValue{"-581:03:49"}}, 1},
	{[]types.Value{types.TimeValue{"591:05:40"}, types.TimeValue{"-752:30:11"}}, 1},
	{[]types.Value{types.TimeValue{"611:19:42"}, types.TimeValue{"74:25:13"}}, 1},
	{[]types.Value{types.TimeValue{"657:24:06"}, types.TimeValue{"56:59:13"}}, 1},
	{[]types.Value{types.TimeValue{"784:01:32"}, types.TimeValue{"-436:26:12"}}, 1},
	{[]types.Value{types.TimeValue{"804:32:49"}, types.TimeValue{"831:04:17"}}, 1},
}
var ourRows = []run.Row{
	{[]types.Value{types.TimeValue{"-824:18:01"}, types.TimeValue{"112:59:35"}}, 1},
	{[]types.Value{types.TimeValue{"-785:36:38"}, types.TimeValue{"-137:17:16"}}, 1},
	{[]types.Value{types.TimeValue{"-772:20:57"}, types.TimeValue{"71:59:30"}}, 1},
	{[]types.Value{types.TimeValue{"-741:15:26"}, types.TimeValue{"-824:06:24"}}, 1},
	{[]types.Value{types.TimeValue{"-711:30:59"}, types.TimeValue{"-397:46:07"}}, 1},
	{[]types.Value{types.TimeValue{"-685:29:52"}, types.TimeValue{"201:59:01"}}, 1},
	{[]types.Value{types.TimeValue{"-666:55:53"}, types.TimeValue{"-132:36:26"}}, 1},
	{[]types.Value{types.TimeValue{"-662:42:35"}, types.TimeValue{"-730:17:54"}}, 1},
	{[]types.Value{types.TimeValue{"-650:19:06"}, types.TimeValue{"-98:27:00"}}, 1},
	{[]types.Value{types.TimeValue{"-622:14:57"}, types.TimeValue{"-228:01:55"}}, 1},
	{[]types.Value{types.TimeValue{"-598:11:54"}, types.TimeValue{"218:57:58"}}, 1},
	{[]types.Value{types.TimeValue{"-596:49:11"}, types.TimeValue{"-120:28:05"}}, 1},
	{[]types.Value{types.TimeValue{"-528:48:35"}, types.TimeValue{"804:42:18"}}, 1},
	{[]types.Value{types.TimeValue{"-512:45:34"}, types.TimeValue{"624:44:00"}}, 1},
	{[]types.Value{types.TimeValue{"-466:34:30"}, types.TimeValue{"-375:59:14"}}, 1},
	{[]types.Value{types.TimeValue{"-442:15:39"}, types.TimeValue{"758:18:25"}}, 1},
	{[]types.Value{types.TimeValue{"-410:21:38"}, types.TimeValue{"63:36:31"}}, 1},
	{[]types.Value{types.TimeValue{"-362:47:22"}, types.TimeValue{"-792:58:17"}}, 1},
	{[]types.Value{types.TimeValue{"-351:46:53"}, types.TimeValue{"-112:48:40"}}, 1},
	{[]types.Value{types.TimeValue{"-313:38:38"}, types.TimeValue{"625:05:21"}}, 1},
	{[]types.Value{types.TimeValue{"-250:38:44"}, types.TimeValue{"-102:01:27"}}, 1},
	{[]types.Value{types.TimeValue{"-247:54:10"}, types.TimeValue{"-115:49:08"}}, 1},
	{[]types.Value{types.TimeValue{"-203:32:21"}, types.TimeValue{"-330:29:49"}}, 1},
	{[]types.Value{types.TimeValue{"-178:55:18"}, types.TimeValue{"636:15:55"}}, 1},
	{[]types.Value{types.TimeValue{"-127:36:58"}, types.TimeValue{"112:23:42"}}, 1},
	{[]types.Value{types.TimeValue{"-36:03:54"}, types.TimeValue{"-200:04:01"}}, 1},
	{[]types.Value{types.TimeValue{"-05:21:15"}, types.TimeValue{"-778:39:01"}}, 1},
	{[]types.Value{types.TimeValue{"18:06:59"}, types.TimeValue{"-12:57:13"}}, 1},
	{[]types.Value{types.TimeValue{"60:49:52"}, types.TimeValue{"29:13:21"}}, 1},
	{[]types.Value{types.TimeValue{"67:35:37"}, types.TimeValue{"-163:40:26"}}, 1},
	{[]types.Value{types.TimeValue{"90:45:49"}, types.TimeValue{"276:03:20"}}, 1},
	{[]types.Value{types.TimeValue{"124:04:30"}, types.TimeValue{"507:39:02"}}, 1},
	{[]types.Value{types.TimeValue{"135:41:08"}, types.TimeValue{"-812:36:16"}}, 1},
	{[]types.Value{types.TimeValue{"135:48:02"}, types.TimeValue{"-166:38:15"}}, 1},
	{[]types.Value{types.TimeValue{"170:50:26"}, types.TimeValue{"465:34:49"}}, 1},
	{[]types.Value{types.TimeValue{"174:22:51"}, types.TimeValue{"587:43:29"}}, 1},
	{[]types.Value{types.TimeValue{"198:14:52"}, types.TimeValue{"200:13:58"}}, 1},
	{[]types.Value{types.TimeValue{"220:24:44"}, types.TimeValue{"-607:19:42"}}, 1},
	{[]types.Value{types.TimeValue{"238:16:14"}, types.TimeValue{"632:05:47"}}, 1},
	{[]types.Value{types.TimeValue{"286:30:14"}, types.TimeValue{"393:52:48"}}, 1},
	{[]types.Value{types.TimeValue{"315:04:59"}, types.TimeValue{"420:20:07"}}, 1},
	{[]types.Value{types.TimeValue{"325:59:39"}, types.TimeValue{"-222:40:59"}}, 1},
	{[]types.Value{types.TimeValue{"327:06:35"}, types.TimeValue{"-830:54:12"}}, 1},
	{[]types.Value{types.TimeValue{"366:29:56"}, types.TimeValue{"-527:11:02"}}, 1},
	{[]types.Value{types.TimeValue{"380:43:51"}, types.TimeValue{"-740:20:15"}}, 1},
	{[]types.Value{types.TimeValue{"384:40:08"}, types.TimeValue{"739:31:11"}}, 1},
	{[]types.Value{types.TimeValue{"390:38:57"}, types.TimeValue{"11:05:46"}}, 1},
	{[]types.Value{types.TimeValue{"410:41:26"}, types.TimeValue{"752:39:02"}}, 1},
	{[]types.Value{types.TimeValue{"452:29:12"}, types.TimeValue{"-590:40:19"}}, 1},
	{[]types.Value{types.TimeValue{"461:43:15"}, types.TimeValue{"-333:06:18"}}, 1},
	{[]types.Value{types.TimeValue{"521:45:58"}, types.TimeValue{"-581:03:49"}}, 1},
	{[]types.Value{types.TimeValue{"522:15:36"}, types.TimeValue{"-114:03:06"}}, 1},
	{[]types.Value{types.TimeValue{"591:05:40"}, types.TimeValue{"-752:30:11"}}, 1},
	{[]types.Value{types.TimeValue{"611:19:42"}, types.TimeValue{"74:25:13"}}, 1},
	{[]types.Value{types.TimeValue{"657:24:06"}, types.TimeValue{"56:59:13"}}, 1},
	{[]types.Value{types.TimeValue{"748:31:39"}, types.TimeValue{"-320:29:19"}}, 1},
	{[]types.Value{types.TimeValue{"783:36:39"}, types.TimeValue{"-37:12:55"}}, 1},
	{[]types.Value{types.TimeValue{"791:12:11"}, types.TimeValue{"-184:53:03"}}, 1},
	{[]types.Value{types.TimeValue{"804:32:49"}, types.TimeValue{"831:04:17"}}, 1},
}
var theirRows = []run.Row{
	{[]types.Value{types.TimeValue{"-650:19:06"}, types.TimeValue{"807:27:02"}}, 1},
	{[]types.Value{types.TimeValue{"-641:17:35"}, types.TimeValue{"548:04:42"}}, 1},
	{[]types.Value{types.TimeValue{"-553:26:28"}, types.TimeValue{"-208:03:57"}}, 1},
	{[]types.Value{types.TimeValue{"-465:01:47"}, types.TimeValue{"434:11:26"}}, 1},
	{[]types.Value{types.TimeValue{"-442:15:39"}, types.TimeValue{"758:18:25"}}, 1},
	{[]types.Value{types.TimeValue{"-415:17:46"}, types.TimeValue{"-554:48:52"}}, 1},
	{[]types.Value{types.TimeValue{"-323:25:39"}, types.TimeValue{"-265:29:29"}}, 1},
	{[]types.Value{types.TimeValue{"-318:20:17"}, types.TimeValue{"806:29:33"}}, 1},
	{[]types.Value{types.TimeValue{"-308:43:24"}, types.TimeValue{"-780:52:57"}}, 1},
	{[]types.Value{types.TimeValue{"-297:21:37"}, types.TimeValue{"-01:28:16"}}, 1},
	{[]types.Value{types.TimeValue{"-271:46:28"}, types.TimeValue{"633:14:36"}}, 1},
	{[]types.Value{types.TimeValue{"-203:32:21"}, types.TimeValue{"-330:29:49"}}, 1},
	{[]types.Value{types.TimeValue{"-188:27:24"}, types.TimeValue{"-373:51:16"}}, 1},
	{[]types.Value{types.TimeValue{"-178:55:18"}, types.TimeValue{"636:15:55"}}, 1},
	{[]types.Value{types.TimeValue{"-134:50:53"}, types.TimeValue{"488:47:21"}}, 1},
	{[]types.Value{types.TimeValue{"06:11:25"}, types.TimeValue{"494:13:52"}}, 1},
	{[]types.Value{types.TimeValue{"18:06:59"}, types.TimeValue{"-12:57:13"}}, 1},
	{[]types.Value{types.TimeValue{"67:35:37"}, types.TimeValue{"347:22:17"}}, 1},
	{[]types.Value{types.TimeValue{"80:49:57"}, types.TimeValue{"589:48:04"}}, 1},
	{[]types.Value{types.TimeValue{"124:04:30"}, types.TimeValue{"-720:08:33"}}, 1},
	{[]types.Value{types.TimeValue{"135:41:08"}, types.TimeValue{"-812:36:16"}}, 1},
	{[]types.Value{types.TimeValue{"135:48:02"}, types.TimeValue{"-303:16:10"}}, 1},
	{[]types.Value{types.TimeValue{"170:50:26"}, types.TimeValue{"465:34:49"}}, 1},
	{[]types.Value{types.TimeValue{"199:40:15"}, types.TimeValue{"-234:10:03"}}, 1},
	{[]types.Value{types.TimeValue{"220:24:44"}, types.TimeValue{"-607:19:42"}}, 1},
	{[]types.Value{types.TimeValue{"235:34:08"}, types.TimeValue{"620:34:59"}}, 1},
	{[]types.Value{types.TimeValue{"272:18:02"}, types.TimeValue{"-450:21:45"}}, 1},
	{[]types.Value{types.TimeValue{"286:30:14"}, types.TimeValue{"393:52:48"}}, 1},
	{[]types.Value{types.TimeValue{"336:26:36"}, types.TimeValue{"-327:13:07"}}, 1},
	{[]types.Value{types.TimeValue{"339:30:09"}, types.TimeValue{"451:34:39"}}, 1},
	{[]types.Value{types.TimeValue{"380:00:13"}, types.TimeValue{"659:34:29"}}, 1},
	{[]types.Value{types.TimeValue{"381:20:06"}, types.TimeValue{"-242:45:50"}}, 1},
	{[]types.Value{types.TimeValue{"390:38:57"}, types.TimeValue{"11:05:46"}}, 1},
	{[]types.Value{types.TimeValue{"390:57:07"}, types.TimeValue{"-689:08:04"}}, 1},
	{[]types.Value{types.TimeValue{"408:15:42"}, types.TimeValue{"-253:14:46"}}, 1},
	{[]types.Value{types.TimeValue{"457:30:03"}, types.TimeValue{"-566:22:01"}}, 1},
	{[]types.Value{types.TimeValue{"461:43:15"}, types.TimeValue{"-633:12:38"}}, 1},
	{[]types.Value{types.TimeValue{"490:08:46"}, types.TimeValue{"426:23:30"}}, 1},
	{[]types.Value{types.TimeValue{"501:43:59"}, types.TimeValue{"788:55:03"}}, 1},
	{[]types.Value{types.TimeValue{"505:13:57"}, types.TimeValue{"-759:13:38"}}, 1},
	{[]types.Value{types.TimeValue{"591:05:40"}, types.TimeValue{"-752:30:11"}}, 1},
	{[]types.Value{types.TimeValue{"611:19:42"}, types.TimeValue{"74:25:13"}}, 1},
	{[]types.Value{types.TimeValue{"646:35:52"}, types.TimeValue{"666:25:44"}}, 1},
	{[]types.Value{types.TimeValue{"664:56:05"}, types.TimeValue{"-804:02:12"}}, 1},
	{[]types.Value{types.TimeValue{"665:49:06"}, types.TimeValue{"689:31:33"}}, 1},
	{[]types.Value{types.TimeValue{"715:33:23"}, types.TimeValue{"-114:32:08"}}, 1},
	{[]types.Value{types.TimeValue{"776:41:04"}, types.TimeValue{"478:44:47"}}, 1},
	{[]types.Value{types.TimeValue{"784:01:32"}, types.TimeValue{"-302:09:44"}}, 1},
	{[]types.Value{types.TimeValue{"784:13:00"}, types.TimeValue{"156:51:29"}}, 1},
	{[]types.Value{types.TimeValue{"804:32:49"}, types.TimeValue{"831:04:17"}}, 1},
	{[]types.Value{types.TimeValue{"820:28:26"}, types.TimeValue{"04:04:14"}}, 1},
	{[]types.Value{types.TimeValue{"821:15:04"}, types.TimeValue{"-05:51:15"}}, 1},
}

func TestMerge(t *testing.T) {
	// This test isn't for verifying code but for stepping through it
	tableName := "FBFIfNfOoi"
	pkCols := []*run.Column{{Name: "nRYVZk", Type: &types.TimeInstance{}}}
	nonPKCols := []*run.Column{{Name: "gL3kqk", Type: &types.TimeInstance{}}}
	mt := &mergeTables{
		tableName: tableName,
		ours:      mustTable(t, nil, tableName, pkCols, nonPKCols, nil),
		theirs:    mustTable(t, nil, tableName, pkCols, nonPKCols, nil),
		base:      mustTable(t, nil, tableName, pkCols, nonPKCols, nil),
		final:     nil,
	}
	err := mt.base.Data.Exec(rowsToInsertString(tableName, baseRows))
	require.NoError(t, err)
	err = mt.ours.Data.Exec(rowsToInsertString(tableName, ourRows))
	require.NoError(t, err)
	err = mt.theirs.Data.Exec(rowsToInsertString(tableName, theirRows))
	require.NoError(t, err)
	mtc, err := mt.ProcessMerge()
	require.NoError(t, err)
	require.NotNil(t, mtc)
	allRows, err := mtc.final.Data.GetAllRows()
	require.NoError(t, err)
	require.NotNil(t, allRows)
}

func mustTable(t *testing.T, parent *run.Commit, name string, pkCols []*run.Column, nonPKCols []*run.Column, indexes []*run.Index) *run.Table {
	tbl, err := run.NewTable(parent, name, pkCols, nonPKCols, indexes)
	require.NoError(t, err)
	return tbl
}

func rowsToInsertString(tableName string, rows []run.Row) string {
	sb := strings.Builder{}
	sb.Grow(128 * 1024)
	for _, row := range rows {
		sb.WriteString(fmt.Sprintf("INSERT INTO `%s` VALUES (%s);", tableName, row.SQLiteString()))
	}
	return sb.String()
}
