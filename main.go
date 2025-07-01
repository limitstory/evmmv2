package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	mod "elastic/modules"
	global "elastic/modules/global"

	remote "k8s.io/kubernetes/pkg/kubelet/cri/remote"
)

func main() {
	const ENDPOINT string = "unix:///var/run/crio/crio.sock"

	podIndex := make(map[string]int64)
	var currentRunningPods []string
	var wg sync.WaitGroup
	var podInfoSet []global.PodData
	var systemInfoSet []global.SystemInfo

	var scaleUpCandidateList []global.ScaleCandidateContainer
	var pauseContainerList []global.PauseContainer
	var checkpointContainerList []global.CheckpointContainer
	var removeContainerList []global.CheckpointContainer

	var avgCheckpointTime []global.CheckpointTime
	var avgImageTime []global.ImageTime
	var avgRemoveTime []global.RemoveTime
	var avgRepairTime []global.RepairTime

	appendCheckpointContainerToChan := make(chan global.CheckpointContainer, 100)
	modifyCheckpointContainerToChan := make(chan global.CheckpointContainer, 100)
	removeContainerToChan := make(chan global.CheckpointContainer, 100)
	repairCandidateToChan := make(chan global.CheckpointContainer, 100)

	// 동시에 실행할 고루틴 수를 제한하는 채널(세마포어 역할)
	semaphore := make(chan struct{}, 2) // 최대 4개의 고루틴만 동시에 실행

	priorityMap := make(map[string]global.PriorityContainer)
	var sortPriority []string

	programStartTime := time.Now().Unix()

	/*
		// kubernetes api 클라이언트 생성하는 모듈
		clientset := mod.InitClient()
		if clientset != nil {
			fmt.Println("123")
		}*/

	//get new internal client service
	client, err := remote.NewRemoteRuntimeService(ENDPOINT, time.Second, nil)
	if err != nil {
		panic(err)
	}

	// execute monitoring & resource management logic
	for {

		// definition of data structure to store
		//var selectContainerId = make([]string, 0)
		//var selectContainerResource = make([]*pb.ContainerResources, 0)

		// get system metrics
		systemInfoSet = mod.GetSystemStatsInfo(systemInfoSet)

		podInfoSet, currentRunningPods = mod.MonitoringPodResources(client, podIndex, podInfoSet, currentRunningPods, systemInfoSet,
			checkpointContainerList, removeContainerList, avgCheckpointTime, avgImageTime, avgRemoveTime, avgRepairTime, removeContainerToChan)

		for i := 0; i < len(currentRunningPods); i++ {
			res := podInfoSet[podIndex[currentRunningPods[i]]].Container[0].Resource
			// main.go 63라인에서 뻗으므로 예외처리 필요함
			if len(res) == 0 {
				continue
			}
			fmt.Println(podInfoSet[podIndex[currentRunningPods[i]]].Name)
			fmt.Println(podInfoSet[podIndex[currentRunningPods[i]]].Container[0].Cgroup.CpuQuota)
			fmt.Println(res[len(res)-1].ConMemUtil)
			fmt.Println(res[len(res)-1].MemoryUsageBytes)
			fmt.Println(podInfoSet[podIndex[currentRunningPods[i]]].Container[0].Cgroup.MemoryLimitInBytes)

			//fmt.Println(res[len(res)-1].CpuUtil)
			//fmt.Println()
		}

		podInfoSet = mod.GetPriorityMetric(podIndex, podInfoSet, currentRunningPods, systemInfoSet, programStartTime)
		podInfoSet, priorityMap, sortPriority = mod.CalculatePriority(podIndex, podInfoSet, currentRunningPods)

		/*
			for _, podName := range currentRunningPods {
				pod := &podInfoSet[podIndex[podName]]

				for i, _ := range pod.Container {
					priority := pod.Container[i].Priority
					fmt.Println(priority)
				}
			}*/

		if false {
			for _, prior := range sortPriority {
				fmt.Println(priorityMap[prior])
			}
		}

		wg.Add(len(podInfoSet))
		for _, pod := range podInfoSet {
			go func(pod global.PodData) {
				defer wg.Done()

				var isScaleCandidate bool
				var isPauseContainer bool
				if len(pod.Container) == 0 {
					return
				}
				for _, scaleCandidate := range scaleUpCandidateList {
					if pod.Name == scaleCandidate.PodName {
						isScaleCandidate = true
						break
					}
				}
				if isScaleCandidate == false && mod.GetContainerFreeze(pod.Uid) == "1" { // exception handling (suspend 안풀렸을 때)
					mod.ContinueContainer(pod.Uid, &pod.Container[0])
				}
				for _, pauseContainer := range pauseContainerList {
					if pod.Name == pauseContainer.PodName {
						isPauseContainer = true
						break
					}
				}
				if isPauseContainer == false && mod.GetContainerFreeze(pod.Uid) == "1" { // exception handling (suspend 안풀렸을 때)
					mod.ContinueContainer(pod.Uid, &pod.Container[0])
				}
			}(pod)
		}
		wg.Wait()

		podInfoSet = mod.DecisionScaleDown(client, podIndex, podInfoSet, currentRunningPods, systemInfoSet)
		podInfoSet, scaleUpCandidateList, pauseContainerList = mod.DecisionScaleUp(client, podIndex, podInfoSet, currentRunningPods,
			systemInfoSet, priorityMap, scaleUpCandidateList, pauseContainerList)

		go func() {
			for {
				select {
				case container1 := <-appendCheckpointContainerToChan:
					found := false
					for i, checkpointContainer := range checkpointContainerList {
						if container1.PodName == checkpointContainer.PodName {
							found = true
							if container1.Timestamp >= checkpointContainer.Timestamp+global.RECHECKPOINT_THRESHOLD {
								checkpointContainerList[i] = container1
								fmt.Println("Modify timestamp: ", checkpointContainerList[i])
							}
						}
					}
					if !found {
						checkpointContainerList = append(checkpointContainerList, container1)
						//fmt.Println("Appended container: ", container1)
					}

				case container2 := <-modifyCheckpointContainerToChan:
					defer func() {
						v := recover()
						fmt.Println("recovered:", v)
					}()
					for i, checkpointContainer := range checkpointContainerList {
						if container2.PodName == checkpointContainer.PodName {
							checkpointContainerList[i] = container2
							//fmt.Println("Data: ", checkpointContainerList[i])

							if container2.IsCheckpoint {
								stats, _ := client.PodSandboxStats(context.TODO(), container2.PodId)
								if stats == nil || len(stats.Linux.Containers) == 0 {
									container2.IsCheckpoint = false
									container2.AbortedCheckpoint = true
									fmt.Println("checkpoint aborted: ", container2.PodName)
									checkpointContainerList = append(checkpointContainerList[:i], checkpointContainerList[i+1:]...)
									break
								} else {
									fmt.Println("checkpoint complete: ", container2.PodName)
									checkpointContainerList[i].EndCheckpointTime = time.Now().Unix()
									var podCheckpointTime global.CheckpointTime
									podCheckpointTime.PodName = container2.PodName
									podCheckpointTime.CheckpointTime = checkpointContainerList[i].EndCheckpointTime - checkpointContainerList[i].StartCheckpointTime
									avgCheckpointTime = append(avgCheckpointTime, podCheckpointTime)
									for j, pauseContainer := range pauseContainerList {
										if container2.PodName == pauseContainer.PodName {
											pauseContainerList[j].IsCheckpoint = true
											break
										}
									}
								}
							}
							break
						}
					}
				}
			}
		}()
		go mod.DecisionCheckpoint(
			systemInfoSet,
			appendCheckpointContainerToChan,
			modifyCheckpointContainerToChan,
			pauseContainerList,
			checkpointContainerList,
			semaphore,
		)

		//go
		scaleUpCandidateList, pauseContainerList, currentRunningPods = mod.DecisionRemoveContainer(
			client,
			systemInfoSet,
			scaleUpCandidateList,
			pauseContainerList,
			checkpointContainerList,
			currentRunningPods,
			len(currentRunningPods),
			priorityMap,
			removeContainerList,
			removeContainerToChan,
		)

		// remove container 모두 채널로 통신가능하도록 변경해야 할듯
		go func() {
			for {
				select {
				case container1 := <-removeContainerToChan:
					var isNotAppend bool
					container1.StartRemoveTime = time.Now().Unix()
					for _, removeContainer := range removeContainerList {
						if removeContainer.PodName == container1.PodName {
							isNotAppend = true
						}
					}
					if !isNotAppend {
						removeContainerList = append(removeContainerList, container1)
					}
				case container2 := <-repairCandidateToChan:
					for i, removeContainer := range removeContainerList {
						if container2.PodName == removeContainer.PodName && (container2.DuringCreateImages || container2.CreateImages || container2.DuringCreateContainer) {
							removeContainerList[i] = container2
							break
						} else if container2.PodName == removeContainer.PodName && container2.CreateContainer {
							defer func() {
								v := recover()
								fmt.Println("recovered:", v)
							}()
							var podRemoveTime global.RemoveTime
							var podImageTime global.ImageTime
							var podRepairTime global.RepairTime

							podRemoveTime.PodName = container2.PodName
							podRemoveTime.RemoveTime = time.Now().Unix() - container2.StartRemoveTime
							avgRemoveTime = append(avgRemoveTime, podRemoveTime)

							podImageTime.PodName = container2.PodName
							podImageTime.ImageTime = container2.EndImageTime - container2.StartImageTime
							avgImageTime = append(avgImageTime, podImageTime)
							podRepairTime.PodName = container2.PodName
							podRepairTime.RepairTime = container2.EndRepairTime - container2.StartRepairTime
							avgRepairTime = append(avgRepairTime, podRepairTime)
							removeContainerList = append(removeContainerList[:i], removeContainerList[i+1:]...)
							break
						}
					}
				}
			}
		}()
		go mod.CreateImageContainer(repairCandidateToChan, client, systemInfoSet, podIndex, podInfoSet, currentRunningPods,
			len(currentRunningPods), priorityMap, removeContainerList)
		go mod.DecisionRepairContainer(repairCandidateToChan, client, systemInfoSet, podIndex, podInfoSet, currentRunningPods,
			priorityMap, removeContainerList)

		//modify.DecisionRepairContainer()
		// cp.MakeContainerFromCheckpoint(checkpointName, "nginx")
		// cp.RestoreContainer("nginx")

		//selectContainerId, selectContainerResource = mod.LimitContainerResources(client, selectContainerId, selectContainerResource)

		// After limiting CPU usage, watch the trend of memory usage.
		//mod.ControlRecursiveContainerResources(client, selectContainerId, selectContainerResource)

		var scaleUpCandidateNameList []string
		var pauseContainerNameList []string
		var checkPointContainerNameList []string
		var removeContainerNameList []string

		for _, scaleUpCandiate := range scaleUpCandidateList {
			scaleUpCandidateNameList = append(scaleUpCandidateNameList, scaleUpCandiate.PodName)
		}
		for _, pauseContainer := range pauseContainerList {
			pauseContainerNameList = append(pauseContainerNameList, pauseContainer.PodName)
		}
		for _, checkPointContainer := range checkpointContainerList {
			checkPointContainerNameList = append(checkPointContainerNameList, checkPointContainer.PodName)
		}
		for _, removeContainer := range removeContainerList {
			removeContainerNameList = append(removeContainerNameList, removeContainer.PodName)
		}

		fmt.Println("scaleUpCandidateList: ", scaleUpCandidateNameList)
		fmt.Println("pauseContainerList: ", pauseContainerNameList)
		fmt.Println("checkPointContainerList: ", checkPointContainerNameList)
		fmt.Println("removeContainerList: ", removeContainerNameList)
		fmt.Println("=====================================================")
		fmt.Println()

		time.Sleep(time.Second)
	}
}

/*
func IsPodRunning(container global.CheckpointContainer, currentRunningPods []string) bool {
	for _, pods := range currentRunningPods {
		if container.PodName == pods {
			return true
		}
	}

	return false
}*/
