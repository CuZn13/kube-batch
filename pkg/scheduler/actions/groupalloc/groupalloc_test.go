/*
Copyright 2017 The Kubernetes Authors.

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

package groupalloc

import (
	"testing"
	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"

	kbv1 "github.com/kubernetes-sigs/kube-batch/pkg/apis/scheduling/v1alpha1"
	"github.com/kubernetes-sigs/kube-batch/pkg/scheduler/api"
	"github.com/kubernetes-sigs/kube-batch/pkg/scheduler/cache"
	"github.com/kubernetes-sigs/kube-batch/pkg/scheduler/conf"
	"github.com/kubernetes-sigs/kube-batch/pkg/scheduler/framework"
	"github.com/kubernetes-sigs/kube-batch/pkg/scheduler/plugins/drf"
	"github.com/kubernetes-sigs/kube-batch/pkg/scheduler/plugins/proportion"
	"github.com/kubernetes-sigs/kube-batch/pkg/scheduler/util"
)

func TestAllocate(t *testing.T) {
	framework.RegisterPluginBuilder("drf", drf.New)
	framework.RegisterPluginBuilder("proportion", proportion.New)
	defer framework.CleanupPluginBuilders()

	tests := []struct {
		name      string
		podGroups []*kbv1.PodGroup
		pods      []*v1.Pod
		nodes     []*v1.Node
		queues    []*kbv1.Queue
		expected  map[string]string
	}{
		{
			name: "one Job with two Pods on one node",
			podGroups: []*kbv1.PodGroup{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "pg1",
						Namespace: "c1",
					},
					Spec: kbv1.PodGroupSpec{
						Queue: "c1",
					},
				},
			},
			pods: []*v1.Pod{
				util.BuildPod("c1", "p1", "", v1.PodPending, util.BuildResourceList("2", "2G"), "pg1", make(map[string]string), make(map[string]string)),
				util.BuildPod("c1", "p2", "", v1.PodPending, util.BuildResourceList("2", "2G"), "pg1", make(map[string]string), make(map[string]string)),
				util.BuildPod("c1", "p3", "", v1.PodPending, util.BuildResourceList("2", "2G"), "pg1", make(map[string]string), make(map[string]string)),
				util.BuildPod("c1", "p4", "", v1.PodPending, util.BuildResourceList("2", "2G"), "pg1", make(map[string]string), make(map[string]string)),
				util.BuildPod("c1", "p5", "", v1.PodPending, util.BuildResourceList("2", "2G"), "pg1", make(map[string]string), make(map[string]string)),
				util.BuildPod("c1", "p6", "", v1.PodPending, util.BuildResourceList("2", "2G"), "pg1", make(map[string]string), make(map[string]string)),
				util.BuildPod("c1", "p7", "", v1.PodPending, util.BuildResourceList("2", "2G"), "pg1", make(map[string]string), make(map[string]string)),
				util.BuildPod("c1", "p8", "", v1.PodPending, util.BuildResourceList("2", "2G"), "pg1", make(map[string]string), make(map[string]string)),
				util.BuildPod("c1", "p9", "", v1.PodPending, util.BuildResourceList("2", "2G"), "pg1", make(map[string]string), make(map[string]string)),
				util.BuildPod("c1", "p10", "", v1.PodPending, util.BuildResourceList("2", "2G"), "pg1", make(map[string]string), make(map[string]string)),
				util.BuildPod("c1", "p11", "", v1.PodPending, util.BuildResourceList("2", "2G"), "pg1", make(map[string]string), make(map[string]string)),
				util.BuildPod("c1", "p12", "", v1.PodPending, util.BuildResourceList("2", "2G"), "pg1", make(map[string]string), make(map[string]string)),
				util.BuildPod("c1", "p13", "", v1.PodPending, util.BuildResourceList("2", "2G"), "pg1", make(map[string]string), make(map[string]string)),
				util.BuildPod("c1", "p14", "", v1.PodPending, util.BuildResourceList("2", "2G"), "pg1", make(map[string]string), make(map[string]string)),
				util.BuildPod("c1", "p15", "", v1.PodPending, util.BuildResourceList("2", "2G"), "pg1", make(map[string]string), make(map[string]string)),
				util.BuildPod("c1", "p16", "", v1.PodPending, util.BuildResourceList("2", "2G"), "pg1", make(map[string]string), make(map[string]string)),
				util.BuildPod("c1", "p17", "", v1.PodPending, util.BuildResourceList("2", "2G"), "pg1", make(map[string]string), make(map[string]string)),
				util.BuildPod("c1", "p18", "", v1.PodPending, util.BuildResourceList("2", "2G"), "pg1", make(map[string]string), make(map[string]string)),
				util.BuildPod("c1", "p19", "", v1.PodPending, util.BuildResourceList("2", "2G"), "pg1", make(map[string]string), make(map[string]string)),
				util.BuildPod("c1", "p20", "", v1.PodPending, util.BuildResourceList("2", "2G"), "pg1", make(map[string]string), make(map[string]string)),
				util.BuildPod("c1", "p21", "", v1.PodPending, util.BuildResourceList("2", "2G"), "pg1", make(map[string]string), make(map[string]string)),
				util.BuildPod("c1", "p22", "", v1.PodPending, util.BuildResourceList("2", "2G"), "pg1", make(map[string]string), make(map[string]string)),
				util.BuildPod("c1", "p23", "", v1.PodPending, util.BuildResourceList("2", "2G"), "pg1", make(map[string]string), make(map[string]string)),
				util.BuildPod("c1", "p24", "", v1.PodPending, util.BuildResourceList("2", "2G"), "pg1", make(map[string]string), make(map[string]string)),
				util.BuildPod("c1", "p25", "", v1.PodPending, util.BuildResourceList("2", "2G"), "pg1", make(map[string]string), make(map[string]string)),
				util.BuildPod("c1", "p26", "", v1.PodPending, util.BuildResourceList("2", "2G"), "pg1", make(map[string]string), make(map[string]string)),
				util.BuildPod("c1", "p27", "", v1.PodPending, util.BuildResourceList("2", "2G"), "pg1", make(map[string]string), make(map[string]string)),
				util.BuildPod("c1", "p28", "", v1.PodPending, util.BuildResourceList("2", "2G"), "pg1", make(map[string]string), make(map[string]string)),
				util.BuildPod("c1", "p29", "", v1.PodPending, util.BuildResourceList("2", "2G"), "pg1", make(map[string]string), make(map[string]string)),
				util.BuildPod("c1", "p30", "", v1.PodPending, util.BuildResourceList("2", "2G"), "pg1", make(map[string]string), make(map[string]string)),
				util.BuildPod("c1", "p31", "", v1.PodPending, util.BuildResourceList("2", "2G"), "pg1", make(map[string]string), make(map[string]string)),
				util.BuildPod("c1", "p32", "", v1.PodPending, util.BuildResourceList("2", "2G"), "pg1", make(map[string]string), make(map[string]string)),
				util.BuildPod("c1", "p33", "", v1.PodPending, util.BuildResourceList("2", "2G"), "pg1", make(map[string]string), make(map[string]string)),
				util.BuildPod("c1", "p34", "", v1.PodPending, util.BuildResourceList("2", "2G"), "pg1", make(map[string]string), make(map[string]string)),
				util.BuildPod("c1", "p35", "", v1.PodPending, util.BuildResourceList("2", "2G"), "pg1", make(map[string]string), make(map[string]string)),
				util.BuildPod("c1", "p36", "", v1.PodPending, util.BuildResourceList("2", "2G"), "pg1", make(map[string]string), make(map[string]string)),
				util.BuildPod("c1", "p37", "", v1.PodPending, util.BuildResourceList("2", "2G"), "pg1", make(map[string]string), make(map[string]string)),
				util.BuildPod("c1", "p38", "", v1.PodPending, util.BuildResourceList("2", "2G"), "pg1", make(map[string]string), make(map[string]string)),
				util.BuildPod("c1", "p39", "", v1.PodPending, util.BuildResourceList("2", "2G"), "pg1", make(map[string]string), make(map[string]string)),
				util.BuildPod("c1", "p40", "", v1.PodPending, util.BuildResourceList("2", "2G"), "pg1", make(map[string]string), make(map[string]string)),
				util.BuildPod("c1", "p41", "", v1.PodPending, util.BuildResourceList("2", "2G"), "pg1", make(map[string]string), make(map[string]string)),
				util.BuildPod("c1", "p42", "", v1.PodPending, util.BuildResourceList("2", "2G"), "pg1", make(map[string]string), make(map[string]string)),
				util.BuildPod("c1", "p43", "", v1.PodPending, util.BuildResourceList("2", "2G"), "pg1", make(map[string]string), make(map[string]string)),
				util.BuildPod("c1", "p44", "", v1.PodPending, util.BuildResourceList("2", "2G"), "pg1", make(map[string]string), make(map[string]string)),
				util.BuildPod("c1", "p45", "", v1.PodPending, util.BuildResourceList("2", "2G"), "pg1", make(map[string]string), make(map[string]string)),
				util.BuildPod("c1", "p46", "", v1.PodPending, util.BuildResourceList("2", "2G"), "pg1", make(map[string]string), make(map[string]string)),
				util.BuildPod("c1", "p47", "", v1.PodPending, util.BuildResourceList("2", "2G"), "pg1", make(map[string]string), make(map[string]string)),
				util.BuildPod("c1", "p48", "", v1.PodPending, util.BuildResourceList("2", "2G"), "pg1", make(map[string]string), make(map[string]string)),
				util.BuildPod("c1", "p49", "", v1.PodPending, util.BuildResourceList("2", "2G"), "pg1", make(map[string]string), make(map[string]string)),
				util.BuildPod("c1", "p50", "", v1.PodPending, util.BuildResourceList("2", "2G"), "pg1", make(map[string]string), make(map[string]string)),
				util.BuildPod("c1", "p51", "", v1.PodPending, util.BuildResourceList("2", "2G"), "pg1", make(map[string]string), make(map[string]string)),
				util.BuildPod("c1", "p52", "", v1.PodPending, util.BuildResourceList("2", "2G"), "pg1", make(map[string]string), make(map[string]string)),
				util.BuildPod("c1", "p53", "", v1.PodPending, util.BuildResourceList("2", "2G"), "pg1", make(map[string]string), make(map[string]string)),
				util.BuildPod("c1", "p54", "", v1.PodPending, util.BuildResourceList("2", "2G"), "pg1", make(map[string]string), make(map[string]string)),
				util.BuildPod("c1", "p55", "", v1.PodPending, util.BuildResourceList("2", "2G"), "pg1", make(map[string]string), make(map[string]string)),
				util.BuildPod("c1", "p56", "", v1.PodPending, util.BuildResourceList("2", "2G"), "pg1", make(map[string]string), make(map[string]string)),
				util.BuildPod("c1", "p57", "", v1.PodPending, util.BuildResourceList("2", "2G"), "pg1", make(map[string]string), make(map[string]string)),
				util.BuildPod("c1", "p58", "", v1.PodPending, util.BuildResourceList("2", "2G"), "pg1", make(map[string]string), make(map[string]string)),
				util.BuildPod("c1", "p59", "", v1.PodPending, util.BuildResourceList("2", "2G"), "pg1", make(map[string]string), make(map[string]string)),
				util.BuildPod("c1", "p60", "", v1.PodPending, util.BuildResourceList("2", "2G"), "pg1", make(map[string]string), make(map[string]string)),
				util.BuildPod("c1", "p61", "", v1.PodPending, util.BuildResourceList("2", "2G"), "pg1", make(map[string]string), make(map[string]string)),
				util.BuildPod("c1", "p62", "", v1.PodPending, util.BuildResourceList("2", "2G"), "pg1", make(map[string]string), make(map[string]string)),
				util.BuildPod("c1", "p63", "", v1.PodPending, util.BuildResourceList("2", "2G"), "pg1", make(map[string]string), make(map[string]string)),
				util.BuildPod("c1", "p64", "", v1.PodPending, util.BuildResourceList("2", "2G"), "pg1", make(map[string]string), make(map[string]string)),
				util.BuildPod("c1", "p65", "", v1.PodPending, util.BuildResourceList("2", "2G"), "pg1", make(map[string]string), make(map[string]string)),
				util.BuildPod("c1", "p66", "", v1.PodPending, util.BuildResourceList("2", "2G"), "pg1", make(map[string]string), make(map[string]string)),
				util.BuildPod("c1", "p67", "", v1.PodPending, util.BuildResourceList("2", "2G"), "pg1", make(map[string]string), make(map[string]string)),
				util.BuildPod("c1", "p68", "", v1.PodPending, util.BuildResourceList("2", "2G"), "pg1", make(map[string]string), make(map[string]string)),
				util.BuildPod("c1", "p69", "", v1.PodPending, util.BuildResourceList("2", "2G"), "pg1", make(map[string]string), make(map[string]string)),
				util.BuildPod("c1", "p70", "", v1.PodPending, util.BuildResourceList("2", "2G"), "pg1", make(map[string]string), make(map[string]string)),
				util.BuildPod("c1", "p71", "", v1.PodPending, util.BuildResourceList("2", "2G"), "pg1", make(map[string]string), make(map[string]string)),
				util.BuildPod("c1", "p72", "", v1.PodPending, util.BuildResourceList("2", "2G"), "pg1", make(map[string]string), make(map[string]string)),
				util.BuildPod("c1", "p73", "", v1.PodPending, util.BuildResourceList("2", "2G"), "pg1", make(map[string]string), make(map[string]string)),
				util.BuildPod("c1", "p74", "", v1.PodPending, util.BuildResourceList("2", "2G"), "pg1", make(map[string]string), make(map[string]string)),
				util.BuildPod("c1", "p75", "", v1.PodPending, util.BuildResourceList("2", "2G"), "pg1", make(map[string]string), make(map[string]string)),
				util.BuildPod("c1", "p76", "", v1.PodPending, util.BuildResourceList("2", "2G"), "pg1", make(map[string]string), make(map[string]string)),
				util.BuildPod("c1", "p77", "", v1.PodPending, util.BuildResourceList("2", "2G"), "pg1", make(map[string]string), make(map[string]string)),
				util.BuildPod("c1", "p78", "", v1.PodPending, util.BuildResourceList("2", "2G"), "pg1", make(map[string]string), make(map[string]string)),
				util.BuildPod("c1", "p79", "", v1.PodPending, util.BuildResourceList("2", "2G"), "pg1", make(map[string]string), make(map[string]string)),
				util.BuildPod("c1", "p80", "", v1.PodPending, util.BuildResourceList("2", "2G"), "pg1", make(map[string]string), make(map[string]string)),
				util.BuildPod("c1", "p81", "", v1.PodPending, util.BuildResourceList("2", "2G"), "pg1", make(map[string]string), make(map[string]string)),
				util.BuildPod("c1", "p82", "", v1.PodPending, util.BuildResourceList("2", "2G"), "pg1", make(map[string]string), make(map[string]string)),
				util.BuildPod("c1", "p83", "", v1.PodPending, util.BuildResourceList("2", "2G"), "pg1", make(map[string]string), make(map[string]string)),
				util.BuildPod("c1", "p84", "", v1.PodPending, util.BuildResourceList("2", "2G"), "pg1", make(map[string]string), make(map[string]string)),
				util.BuildPod("c1", "p85", "", v1.PodPending, util.BuildResourceList("2", "2G"), "pg1", make(map[string]string), make(map[string]string)),
				util.BuildPod("c1", "p86", "", v1.PodPending, util.BuildResourceList("2", "2G"), "pg1", make(map[string]string), make(map[string]string)),
				util.BuildPod("c1", "p87", "", v1.PodPending, util.BuildResourceList("2", "2G"), "pg1", make(map[string]string), make(map[string]string)),
				util.BuildPod("c1", "p88", "", v1.PodPending, util.BuildResourceList("2", "2G"), "pg1", make(map[string]string), make(map[string]string)),
				util.BuildPod("c1", "p89", "", v1.PodPending, util.BuildResourceList("2", "2G"), "pg1", make(map[string]string), make(map[string]string)),
				util.BuildPod("c1", "p90", "", v1.PodPending, util.BuildResourceList("2", "2G"), "pg1", make(map[string]string), make(map[string]string)),
				util.BuildPod("c1", "p91", "", v1.PodPending, util.BuildResourceList("2", "2G"), "pg1", make(map[string]string), make(map[string]string)),
				util.BuildPod("c1", "p92", "", v1.PodPending, util.BuildResourceList("2", "2G"), "pg1", make(map[string]string), make(map[string]string)),
				util.BuildPod("c1", "p93", "", v1.PodPending, util.BuildResourceList("2", "2G"), "pg1", make(map[string]string), make(map[string]string)),
				util.BuildPod("c1", "p94", "", v1.PodPending, util.BuildResourceList("2", "2G"), "pg1", make(map[string]string), make(map[string]string)),
				util.BuildPod("c1", "p95", "", v1.PodPending, util.BuildResourceList("2", "2G"), "pg1", make(map[string]string), make(map[string]string)),
				util.BuildPod("c1", "p96", "", v1.PodPending, util.BuildResourceList("2", "2G"), "pg1", make(map[string]string), make(map[string]string)),
				util.BuildPod("c1", "p97", "", v1.PodPending, util.BuildResourceList("2", "2G"), "pg1", make(map[string]string), make(map[string]string)),
				util.BuildPod("c1", "p98", "", v1.PodPending, util.BuildResourceList("2", "2G"), "pg1", make(map[string]string), make(map[string]string)),
				util.BuildPod("c1", "p99", "", v1.PodPending, util.BuildResourceList("2", "2G"), "pg1", make(map[string]string), make(map[string]string)),
				util.BuildPod("c1", "p110", "", v1.PodPending, util.BuildResourceList("2", "2G"), "pg1", make(map[string]string), make(map[string]string)),
			},
			nodes: []*v1.Node{
				util.BuildNode("n1", util.BuildResourceList("60", "512Gi"), make(map[string]string)),

			},
			queues: []*kbv1.Queue{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "c1",
					},
					Spec: kbv1.QueueSpec{
						Weight: 1,
					},
				},
			},
			expected: map[string]string{
				"c1/p1": "n1",
				"c1/p2": "n2",
			},
		},
	}

	groupalloc := New()

	for _, test := range tests {
		binder := &util.FakeBinder{
			Binds:   map[string]string{},
			Channel: make(chan string),
		}
		schedulerCache := &cache.SchedulerCache{
			Nodes:         make(map[string]*api.NodeInfo),
			Jobs:          make(map[api.JobID]*api.JobInfo),
			Queues:        make(map[api.QueueID]*api.QueueInfo),
			Binder:        binder,
			StatusUpdater: &util.FakeStatusUpdater{},
			VolumeBinder:  &util.FakeVolumeBinder{},

			Recorder: record.NewFakeRecorder(100),
		}
		for _, node := range test.nodes {
			schedulerCache.AddNode(node)
		}
		for _, pod := range test.pods {
			schedulerCache.AddPod(pod)
		}

		for _, ss := range test.podGroups {
			schedulerCache.AddPodGroup(ss)
		}

		for _, q := range test.queues {
			schedulerCache.AddQueue(q)
		}

		trueValue := true
		ssn := framework.OpenSession(schedulerCache, []conf.Tier{
			{
				Plugins: []conf.PluginOption{
					{
						Name:               "drf",
						EnabledPreemptable: &trueValue,
						EnabledJobOrder:    &trueValue,
					},
					{
						Name:               "proportion",
						EnabledQueueOrder:  &trueValue,
						EnabledReclaimable: &trueValue,
					},
				},
			},
		})
		defer framework.CloseSession(ssn)

		groupalloc.Execute(ssn)
		//for i := 0; i < len(test.expected); i++ {
		//	select {
		//	case <-binder.Channel:
		//	case <-time.After(3 * time.Second):
		//		t.Errorf("Failed to get binding request.")
		//	}
		//}
		//
		//if !reflect.DeepEqual(test.expected, binder.Binds) {
		//	t.Errorf("case %d (%s): expected: %v, got %v ", i, test.name, test.expected, binder.Binds)
		//}
	}
}
