/*
Copyright 2019 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package actions

import (
	"github.com/kubernetes-sigs/kube-batch/pkg/scheduler/framework"

	"github.com/kubernetes-sigs/kube-batch/pkg/scheduler/actions/backfill"
	"github.com/kubernetes-sigs/kube-batch/pkg/scheduler/actions/preempt"
	"github.com/kubernetes-sigs/kube-batch/pkg/scheduler/actions/reclaim"
	"github.com/kubernetes-sigs/kube-batch/pkg/scheduler/actions/groupalloc"
)

func init() {
	framework.RegisterAction(reclaim.New())
	//framework.RegisterAction(allocate.New())
	framework.RegisterAction(groupalloc.New())
	framework.RegisterAction(backfill.New())
	framework.RegisterAction(preempt.New())
}
