// MIT License
//
// Copyright (c) 2019 Thibault NORMAND
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package db

import "strings"

// SortDirection is the enumeration for sort
type SortDirection int

const (
	// Ascending sort from bottom to up
	Ascending SortDirection = iota + 1
	// Descending sort from up to bottom
	Descending
)

var sortDirections = [...]string{
	"asc",
	"desc",
}

func (m SortDirection) String() string {
	return sortDirections[m-1]
}

// -----------------------------------------------------------------------------

// SortParameter contains a field name with sort direction
type SortParameter struct {
	FieldName string
	Direction SortDirection
}

// SortParameters contains a list of sort parameters
type SortParameters []SortParameter

// SortConverter convert a list of string to a SortParameters instance
func SortConverter(sorts []string) SortParameters {
	params := make(SortParameters, 0, len(sorts))

	for _, cond := range sorts {
		cond = strings.TrimSpace(cond)
		if len(cond) > 0 {
			switch cond[0] {
			case '-':
				params = append(params, SortParameter{cond[1:], Descending})
			case '+':
				params = append(params, SortParameter{cond[1:], Ascending})
			default:
				params = append(params, SortParameter{cond, Ascending})
			}
		}
	}

	return params
}
