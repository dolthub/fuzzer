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

package types

import (
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/vitess/go/sqltypes"

	"github.com/dolthub/fuzzer/errors"
	"github.com/dolthub/fuzzer/ranges"
)

// Column represents a table column in dolt.
type Column struct {
	Name string
	Type TypeInstance
}

func ConvertGMSSchemaToFuzzerSchema(schema sql.Schema) (pkCols []Column, nonPKCols []Column, err error) {
	for _, sqlCol := range schema {
		fuzzerCol := Column{Name: sqlCol.Name}
		switch sqlCol.Type.Type() {
		case sqltypes.Int8:
			fuzzerCol.Type = &TinyintInstance{}
		case sqltypes.Uint8:
			fuzzerCol.Type = &TinyintUnsignedInstance{}
		case sqltypes.Int16:
			fuzzerCol.Type = &SmallintInstance{}
		case sqltypes.Uint16:
			fuzzerCol.Type = &SmallintUnsignedInstance{}
		case sqltypes.Int24:
			fuzzerCol.Type = &MediumintInstance{}
		case sqltypes.Uint24:
			fuzzerCol.Type = &MediumintUnsignedInstance{}
		case sqltypes.Int32:
			fuzzerCol.Type = &IntInstance{}
		case sqltypes.Uint32:
			fuzzerCol.Type = &IntUnsignedInstance{}
		case sqltypes.Int64:
			fuzzerCol.Type = &BigintInstance{}
		case sqltypes.Uint64:
			fuzzerCol.Type = &BigintUnsignedInstance{}
		case sqltypes.Float32:
			fuzzerCol.Type = &FloatInstance{}
		case sqltypes.Float64:
			fuzzerCol.Type = &DoubleInstance{}
		case sqltypes.Timestamp:
			fuzzerCol.Type = &TimestampInstance{}
		case sqltypes.Date:
			fuzzerCol.Type = &DateInstance{}
		case sqltypes.Time:
			fuzzerCol.Type = &TimeInstance{}
		case sqltypes.Datetime:
			fuzzerCol.Type = &DatetimeInstance{}
		case sqltypes.Year:
			fuzzerCol.Type = &YearInstance{}
		case sqltypes.Decimal:
			decType := sqlCol.Type.(sql.DecimalType)
			fuzzerCol.Type = &DecimalInstance{int(decType.Precision()), int(decType.Scale())}
		case sqltypes.Text:
			stringType := sqlCol.Type.(sql.StringType)
			fuzzerCol.Type = &TextInstance{ranges.NewInt([]int64{0, stringType.MaxByteLength()}), stringType.Collation()}
		case sqltypes.Blob:
			stringType := sqlCol.Type.(sql.StringType)
			fuzzerCol.Type = &BlobInstance{ranges.NewInt([]int64{0, stringType.MaxByteLength()})}
		case sqltypes.VarChar:
			stringType := sqlCol.Type.(sql.StringType)
			fuzzerCol.Type = &VarcharInstance{ranges.NewInt([]int64{0, stringType.MaxByteLength()}), stringType.Collation()}
		case sqltypes.VarBinary:
			stringType := sqlCol.Type.(sql.StringType)
			fuzzerCol.Type = &VarbinaryInstance{ranges.NewInt([]int64{0, stringType.MaxByteLength()})}
		case sqltypes.Char:
			stringType := sqlCol.Type.(sql.StringType)
			fuzzerCol.Type = &CharInstance{int(stringType.MaxByteLength()), stringType.Collation()}
		case sqltypes.Binary:
			stringType := sqlCol.Type.(sql.StringType)
			fuzzerCol.Type = &BinaryInstance{int(stringType.MaxByteLength())}
		case sqltypes.Bit:
			bitType := sqlCol.Type.(sql.BitType)
			fuzzerCol.Type = &BitInstance{uint64(bitType.NumberOfBits())}
		case sqltypes.Enum:
			enumType := sqlCol.Type.(sql.EnumType)
			vals := enumType.Values()
			elementMap := make(map[string]uint16)
			elementMap[""] = 0
			for i, val := range vals {
				elementMap[val] = uint16(i + 1)
			}
			fuzzerCol.Type = &EnumInstance{vals, elementMap, enumType.Collation()}
		case sqltypes.Set:
			setType := sqlCol.Type.(sql.SetType)
			vals := setType.Values()
			elementMap := make(map[string]uint64)
			elementMap[""] = 0
			for i, val := range vals {
				elementMap[val] = 1 << uint64(i)
			}
			fuzzerCol.Type = &SetInstance{vals, elementMap, setType.Collation()}
		default:
			return nil, nil, errors.New(fmt.Sprintf("Unknown type: '%s'", sqlCol.Type.String()))
		}
		if sqlCol.PrimaryKey {
			pkCols = append(pkCols, fuzzerCol)
		} else {
			nonPKCols = append(nonPKCols, fuzzerCol)
		}
	}
	return pkCols, nonPKCols, nil
}
