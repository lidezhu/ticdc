// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package v2

import (
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/pingcap/ticdc/logservice/logpuller"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/errors"
)

// LogPullerObservability returns a point-in-time observability snapshot of the
// local log puller instance.
//
// @Summary Get the local log puller observability snapshot
// @Description This API returns the local node's log puller runtime summary, worker/store snapshots and aggregated failures.
// @Tags common,v2
// @Accept json
// @Produce json
// @Param sample_limit query int false "Maximum number of sampled slow regions / unlocked ranges to return"
// @Success 200 {object} logpuller.ObservabilitySnapshot
// @Failure 500,400 {object} model.HTTPError
// @Router /api/v2/debug/logpuller [get]
func (h *OpenAPIV2) LogPullerObservability(c *gin.Context) {
	subClient, ok := appcontext.LookupService[logpuller.SubscriptionClient](appcontext.SubscriptionClient)
	if !ok || subClient == nil {
		_ = c.Error(errors.New("subscription client not found"))
		return
	}

	sampleLimit := 0
	if value := c.Query("sample_limit"); value != "" {
		parsed, err := strconv.Atoi(value)
		if err != nil {
			_ = c.Error(errors.ErrAPIInvalidParam.GenWithStack("invalid sample_limit: %s", value))
			return
		}
		sampleLimit = parsed
	}
	c.IndentedJSON(http.StatusOK, subClient.GetObservabilitySnapshot(sampleLimit))
}
