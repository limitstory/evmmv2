package modules

import (
	global "elastic/modules/global"
	"math"
	"sort"
	"time"
)

func GetPriorityMetric(podIndex map[string]int64, podInfoSet []global.PodData, currentRunningPods []string, systemInfoSet []global.SystemInfo, programStartTime int64) []global.PodData {
	for _, podName := range currentRunningPods {
		pod := &podInfoSet[podIndex[podName]]

		// get priority metric of containers
		for i, container := range pod.Container {
			var sumConMemUtil float64 = 0.0
			var sumNodeMemUtil float64 = 0.0
			var sumVarConMemUtil float64 = 0.0
			var sumCpuUtil float64 = 0.0

			for j := 1; j <= int(container.TimeWindow); j++ {
				conMemUtil := container.Resource[len(container.Resource)-j].ConMemUtil
				nodeMemUtil := container.Resource[len(container.Resource)-j].NodeMemUtil
				cpuUtil := container.Resource[len(container.Resource)-j].CpuUtil

				sumConMemUtil += conMemUtil
				sumNodeMemUtil += nodeMemUtil
				sumCpuUtil += cpuUtil
			}
			pod.Container[i].Priority.AvgConMemUtil = sumConMemUtil / float64(container.TimeWindow)
			pod.Container[i].Priority.AvgNodeMemUtil = sumNodeMemUtil / float64(container.TimeWindow)
			pod.Container[i].Priority.AvgCpuUtil = sumCpuUtil / float64(container.TimeWindow)

			for j := 1; j <= int(container.TimeWindow); j++ {
				conMemUtil := pod.Container[i].Resource[len(container.Resource)-j].ConMemUtil
				sumVarConMemUtil += math.Pow(container.Priority.AvgConMemUtil-conMemUtil, 2)
			}
			pod.Container[i].Priority.VarConMemUtil = sumVarConMemUtil / float64(container.TimeWindow)

			// 우선순위 ID 계산
			startTime := (container.StartedAt / global.NANOSECONDS) - programStartTime
			if startTime <= 0 {
				pod.Container[i].Priority.PriorityID = 1
			} else {
				pod.Container[i].Priority.PriorityID = 1 / float64(startTime)
			}

			if global.NumOfTotalScale == 0 {
				pod.Container[i].Priority.Penalty = 0
			} else {
				pod.Container[i].Priority.Penalty = 1 - (float64(container.NumOfScale) / float64(global.NumOfTotalScale))
			}

			// 보상 계산 (다운타임 제외)
			uptime := time.Now().Unix() - (container.StartedAt / global.NANOSECONDS) - container.DownTime
			if uptime > 0 {
				pod.Container[i].Priority.Reward = float64(container.PauseTime) / float64(uptime)
			} else {
				pod.Container[i].Priority.Reward = 0
			}
		}
	}

	return podInfoSet
}

func CalculatePriority(podIndex map[string]int64, podInfoSet []global.PodData, currentRunningPods []string) ([]global.PodData, map[string]global.PriorityContainer, []string) {

	priorityMap := make(map[string]global.PriorityContainer)

	for _, podName := range currentRunningPods {
		pod := &podInfoSet[podIndex[podName]]

		for i, _ := range pod.Container {
			priority := pod.Container[i].Priority

			// calculate resource score
			resourceScore := 0.0
			for i, value := range []float64{
				// AvgConMemUtil, AvgUnusedConSwapUtil, AvgNodeMemUtil, VarConMemUtil, AvgCPUUtil
				priority.AvgConMemUtil,
				priority.AvgNodeMemUtil,
				priority.VarConMemUtil,
				priority.AvgCpuUtil,
			} {
				resourceScore += value * global.RESOURCE_VECTOR[i]
			}

			// calcurate priorityScore
			priorityScore := 0.0
			for i, value := range []float64{
				// resourceScore, PriorityID, Penalty, Reward
				resourceScore,
				priority.PriorityID,
				priority.Penalty,
				priority.Reward,
			} {
				priorityScore += value * global.PRIORITY_VECTOR[i]
			}
			pod.Container[i].Priority.PriorityScore = priorityScore

			priorityMap[pod.Id] = global.PriorityContainer{
				PodName:       podName,
				ContainerName: pod.Container[i].Name,
				ContainerId:   pod.Container[i].Id,
				Priority:      priorityScore,
			}
		}
	}

	sortPriority := make([]string, 0, len(priorityMap))

	for k := range priorityMap {
		// 우선순위에 따라 정렬하고 리턴
		sortPriority = append(sortPriority, k)
	}
	sort.SliceStable(sortPriority, func(i, j int) bool {
		return priorityMap[sortPriority[i]].Priority > priorityMap[sortPriority[j]].Priority
	})

	return podInfoSet, priorityMap, sortPriority
}
