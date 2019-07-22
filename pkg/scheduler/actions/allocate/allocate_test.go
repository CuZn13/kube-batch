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

package allocate

import (
	"testing"
	"time"

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
	"strconv"
	"github.com/kubernetes-sigs/kube-batch/pkg/scheduler/plugins/gang"
	"github.com/golang/glog"
)

func TestAllocate(t *testing.T) {
	framework.RegisterPluginBuilder("drf", drf.New)
	framework.RegisterPluginBuilder("proportion", proportion.New)
	framework.RegisterPluginBuilder("gang", gang.New)
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
						MinMember:10,
					},
				},
			},
			pods: []*v1.Pod{
				util.BuildPod("c1", "p1", "", v1.PodPending, util.BuildResourceList("1", "1G"), "pg1", make(map[string]string), make(map[string]string)),
				util.BuildPod("c1", "p2", "", v1.PodPending, util.BuildResourceList("1", "1G"), "pg1", make(map[string]string), make(map[string]string)),
			},
			nodes: []*v1.Node{
				util.BuildNode("n1", util.BuildResourceList("2", "4Gi"), make(map[string]string)),
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
				"c1/p2": "n1",
			},
		},
		//{
		//	name: "two Jobs on one node",
		//	podGroups: []*kbv1.PodGroup{
		//		{
		//			ObjectMeta: metav1.ObjectMeta{
		//				Name:      "pg1",
		//				Namespace: "c1",
		//			},
		//			Spec: kbv1.PodGroupSpec{
		//				Queue: "c1",
		//			},
		//		},
		//		{
		//			ObjectMeta: metav1.ObjectMeta{
		//				Name:      "pg2",
		//				Namespace: "c2",
		//			},
		//			Spec: kbv1.PodGroupSpec{
		//				Queue: "c2",
		//			},
		//		},
		//	},
		//
		//	pods: []*v1.Pod{
		//		// pending pod with owner1, under c1
		//		util.BuildPod("c1", "p1", "", v1.PodPending, util.BuildResourceList("1", "1G"), "pg1", make(map[string]string), make(map[string]string)),
		//		// pending pod with owner1, under c1
		//		util.BuildPod("c1", "p2", "", v1.PodPending, util.BuildResourceList("1", "1G"), "pg1", make(map[string]string), make(map[string]string)),
		//		// pending pod with owner2, under c2
		//		util.BuildPod("c2", "p1", "", v1.PodPending, util.BuildResourceList("1", "1G"), "pg2", make(map[string]string), make(map[string]string)),
		//		// pending pod with owner, under c2
		//		util.BuildPod("c2", "p2", "", v1.PodPending, util.BuildResourceList("1", "1G"), "pg2", make(map[string]string), make(map[string]string)),
		//	},
		//	nodes: []*v1.Node{
		//		util.BuildNode("n1", util.BuildResourceList("2", "4G"), make(map[string]string)),
		//	},
		//	queues: []*kbv1.Queue{
		//		{
		//			ObjectMeta: metav1.ObjectMeta{
		//				Name: "c1",
		//			},
		//			Spec: kbv1.QueueSpec{
		//				Weight: 1,
		//			},
		//		},
		//		{
		//			ObjectMeta: metav1.ObjectMeta{
		//				Name: "c2",
		//			},
		//			Spec: kbv1.QueueSpec{
		//				Weight: 1,
		//			},
		//		},
		//	},
		//	expected: map[string]string{
		//		"c2/p1": "n1",
		//		"c1/p1": "n1",
		//	},
		//},
	}

	allocate := New()

	for i, test := range tests {
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
		podnum:=int(500)
		nodenum:=int(5000)
		for i=0;i<nodenum;i++{
			n:=util.BuildNode("n"+strconv.Itoa(i), util.BuildResourceList("64", "128Gi"), make(map[string]string))
			schedulerCache.AddNode(n)
		}
		for i=0;i<podnum;i++{
			p:=util.BuildPod("c1", "p"+strconv.Itoa(i), "", v1.PodPending, util.BuildResourceList("4", "4G"), "pg1", make(map[string]string), make(map[string]string))
			schedulerCache.AddPod(p)
		}

		for _, ss := range test.podGroups {
			schedulerCache.AddPodGroup(ss)
		}

		for _, q := range test.queues {
			schedulerCache.AddQueue(q)
		}

		trueValue := true
		begin:=time.Now()
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
					{
						Name:               "gang",
						EnabledPreemptable: &trueValue,
						EnabledJobOrder:    &trueValue,
						EnabledJobReady:    &trueValue,
					},
				},
			},
		})
		defer framework.CloseSession(ssn)

		allocate.Execute(ssn)
		glog.V(1).Infof("%s allocate end ...",time.Now().Sub(begin))
		glog.Flush()
		for i := 0; i < podnum; i++ {
			select {
			case <-binder.Channel:
			case <-time.After(3 * time.Second):
				t.Errorf("Failed to get binding request.")
			}
		}

		if len(binder.Binds)==podnum{
			glog.V(1).Infof("test OK")
			glog.Flush()
		}
		//
		//if !reflect.DeepEqual(test.expected, binder.Binds) {
		//	t.Errorf("case %d (%s): expected: %v, got %v ", i, test.name, test.expected, binder.Binds)
		//}
	}
}
