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
	"fmt"

	"github.com/golang/glog"

	"github.com/kubernetes-sigs/kube-batch/pkg/scheduler/api"
	"github.com/kubernetes-sigs/kube-batch/pkg/scheduler/framework"
	"github.com/kubernetes-sigs/kube-batch/pkg/scheduler/util"
)

type groupAllocAction struct {
	ssn *framework.Session
}

func New() *groupAllocAction {
	return &groupAllocAction{}
}

func (alloc *groupAllocAction) Name() string {
	return "groupalloc"
}

func (alloc *groupAllocAction) Initialize() {}

func (alloc *groupAllocAction) Execute(ssn *framework.Session) {
	glog.V(3).Infof("Enter GroupAlloc ...")
	defer glog.V(3).Infof("Leaving GroupAlloc ...")

	queues := util.NewPriorityQueue(ssn.QueueOrderFn)
	jobsMap := map[api.QueueID]*util.PriorityQueue{}

	for _, job := range ssn.Jobs {
		if queue, found := ssn.Queues[job.Queue]; found {
			queues.Push(queue)
		} else {
			glog.Warningf("Skip adding Job <%s/%s> because its queue %s is not found",
				job.Namespace, job.Name, job.Queue)
			continue
		}

		if _, found := jobsMap[job.Queue]; !found {
			jobsMap[job.Queue] = util.NewPriorityQueue(ssn.JobOrderFn)
		}

		glog.V(4).Infof("Added Job <%s/%s> into Queue <%s>", job.Namespace, job.Name, job.Queue)
		jobsMap[job.Queue].Push(job)
	}

	glog.V(3).Infof("Try to GroupAlloc resource to %d Queues", len(jobsMap))

	pendingTasks := map[api.JobID]*util.PriorityQueue{}

	allNodes := util.GetNodeList(ssn.Nodes)

	predicateFn := func(task *api.TaskInfo, node *api.NodeInfo) error {
		// Check for Resource Predicate
		// TODO: We could not allocate resource to task from both node.Idle and node.Releasing now,
		// after it is done, we could change the following compare to:
		// clonedNode := node.Idle.Clone()
		// if !task.InitResreq.LessEqual(clonedNode.Add(node.Releasing)) {
		//    ...
		// }
		if !task.InitResreq.LessEqual(node.Idle) && !task.InitResreq.LessEqual(node.Releasing) {
			return fmt.Errorf("task <%s/%s> ResourceFit failed on node <%s>",
				task.Namespace, task.Name, node.Name)
		}

		return ssn.PredicateFn(task, node)
	}

	for {
		if queues.Empty() {
			break
		}

		queue := queues.Pop().(*api.QueueInfo)
		if ssn.Overused(queue) {
			glog.V(3).Infof("Queue <%s> is overused, ignore it.", queue.Name)
			continue
		}

		jobs, found := jobsMap[queue.UID]

		glog.V(3).Infof("Try to allocate resource to Jobs in Queue <%v>", queue.Name)

		if !found || jobs.Empty() {
			glog.V(4).Infof("Can not find jobs for queue %s.", queue.Name)
			continue
		}

		job := jobs.Pop().(*api.JobInfo)
		if _, found := pendingTasks[job.UID]; !found {
			tasks := util.NewPriorityQueue(ssn.TaskOrderFn)
			for _, task := range job.TaskStatusIndex[api.Pending] {
				// Skip BestEffort task in 'allocate' action.
				if task.Resreq.IsEmpty() {
					glog.V(4).Infof("Task <%v/%v> is BestEffort task, skip it.",
						task.Namespace, task.Name)
					continue
				}

				tasks.Push(task)
			}
			pendingTasks[job.UID] = tasks
		}
		tasks := pendingTasks[job.UID]

		glog.V(3).Infof("Try to allocate resource to %d tasks of Job <%v/%v>",
			tasks.Len(), job.Namespace, job.Name)

		var priorityList util.HostPriorityList
		var i = int(0)
		if !tasks.Empty(){
			task := tasks.Pop().(*api.TaskInfo)
			predicateNodes := util.PredicateNodes(task, allNodes, predicateFn)
			glog.V(4).Infof("predicateNodes size : %d",len(predicateNodes))
			if len(predicateNodes) == 0 {
				break
			}
			var err error
			priorityList, err = util.PrioritizeNodes(task, predicateNodes, ssn.NodePrioritizers())
			glog.V(4).Infof("priorityList size : %d",len(priorityList))
			if err != nil {
				glog.Errorf("Prioritize Nodes for task %s err: %v", task.UID, err)
				break
			}
			if len(priorityList) == 0 {
				break
			}
			tasks.Push(task)
		}
		for !tasks.Empty() {
			task := tasks.Pop().(*api.TaskInfo)

			glog.V(4).Infof("There are <%d> nodes for Job <%v/%v>",
				len(ssn.Nodes), job.Namespace, job.Name)

			if len(job.NodesFitDelta) > 0 {
				job.NodesFitDelta = make(api.NodeResourceMap)
			}
			glog.V(4).Infof("priorityList[i].Host : %v",priorityList[i].Host)
			node := ssn.Nodes[priorityList[i].Host]
			i=i+1
			// Allocate idle resource to the task.
			if task.InitResreq.LessEqual(node.Idle) {
				glog.V(3).Infof("Binding Task <%v/%v> to node <%v>",
					task.Namespace, task.Name, node.Name)
				if err := ssn.Allocate(task, node.Name); err != nil {
					glog.Errorf("Failed to bind Task %v on %v in Session %v, err: %v",
						task.UID, node.Name, ssn.UID, err)
				}
			} else {
				//store information about missing resources
				job.NodesFitDelta[node.Name] = node.Idle.Clone()
				job.NodesFitDelta[node.Name].FitDelta(task.InitResreq)
				glog.V(3).Infof("Predicates failed for task <%s/%s> on node <%s> with limited resources",
					task.Namespace, task.Name, node.Name)

				// Allocate releasing resource to the task if any.
				if task.InitResreq.LessEqual(node.Releasing) {
					glog.V(3).Infof("Pipelining Task <%v/%v> to node <%v> for <%v> on <%v>",
						task.Namespace, task.Name, node.Name, task.InitResreq, node.Releasing)
					if err := ssn.Pipeline(task, node.Name); err != nil {
						glog.Errorf("Failed to pipeline Task %v on %v in Session %v",
							task.UID, node.Name, ssn.UID)
					}
				}
			}
			if len(priorityList) <= i  {
				jobs.Push(job)
				break
			}
			if ssn.JobReady(job){
				jobs.Push(job)
				break
			}
		}

		// Added Queue back until no job in Queue.
		queues.Push(queue)
	}
}

func (alloc *groupAllocAction) UnInitialize() {}
