// Copyright (c) 2013-2018, OVH SAS.
// All rights reserved.

// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:

//   * Redistributions of source code must retain the above copyright
//     notice, this list of conditions and the following disclaimer.
//   * Redistributions in binary form must reproduce the above copyright
//     notice, this list of conditions and the following disclaimer in the
//     documentation and/or other materials provided with the distribution.
//   * Neither the name of OVH SAS nor the
//     names of its contributors may be used to endorse or promote products
//     derived from this software without specific prior written permission.

// THIS SOFTWARE IS PROVIDED BY OVH SAS AND CONTRIBUTORS ``AS IS'' AND ANY
// EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
// WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
// DISCLAIMED. IN NO EVENT SHALL OVH SAS AND CONTRIBUTORS BE LIABLE FOR ANY
// DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
// (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
// LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
// ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
// SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

package flags

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/scraly/go.pkg/log"
	"github.com/fatih/structs"
)

// AsEnvVariables sets struct values from environment variables
func AsEnvVariables(o interface{}, prefix string, skipCommented bool) map[string]string {
	r := map[string]string{}
	prefix = strings.ToUpper(prefix)
	delim := "_"
	if prefix == "" {
		delim = ""
	}
	fields := structs.Fields(o)
	for _, f := range fields {
		if skipCommented {
			tag := f.Tag("commented")
			if tag != "" {
				commented, err := strconv.ParseBool(tag)
				log.CheckErr("Unable to parse tag value", err)
				if commented {
					continue
				}
			}
		}
		if structs.IsStruct(f.Value()) {
			rf := AsEnvVariables(f.Value(), prefix+delim+f.Name(), skipCommented)
			for k, v := range rf {
				r[k] = v
			}
		} else {
			r[prefix+"_"+strings.ToUpper(f.Name())] = fmt.Sprintf("%v", f.Value())
		}
	}
	return r
}
