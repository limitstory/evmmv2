package modules

import (
	"fmt"
	"os/exec"
	"time"

	global "elastic/modules/global"

	internalapi "k8s.io/cri-api/pkg/apis"
)

func DecisionRemoveContainer(
	client internalapi.RuntimeService,
	systemInfoSet []global.SystemInfo,
	scaleUpCandidateList []global.ScaleCandidateContainer,
	pauseContainerList []global.PauseContainer,
	checkPointContainerList []global.CheckpointContainer,
	currentRunningPods []string, lenghOfCurrentRunningPods int,
	priorityMap map[string]global.PriorityContainer,
	removeContainerList []global.CheckpointContainer,
	removeContainerToChan chan global.CheckpointContainer,
) (
	[]global.ScaleCandidateContainer,
	[]global.PauseContainer,
	[]string,
) {

	removeCandidateContainerList := AppendRemoveContainerList(&scaleUpCandidateList, &pauseContainerList, &checkPointContainerList)
	// 체크포인트 한 컨테이너 중 재시작한 컨테이너가 있으면 remove해야 한다.
	currentRunningPods = PerformRemoveContainers(client, removeContainerToChan, removeCandidateContainerList, currentRunningPods)

	return scaleUpCandidateList, pauseContainerList, currentRunningPods
}

func AppendRemoveContainerList(
	scaleUpCandidateList *[]global.ScaleCandidateContainer,
	pauseContainerList *[]global.PauseContainer,
	checkpointContainerList *[]global.CheckpointContainer,
) []global.CheckpointContainer {
	var newPauseContainerList []global.PauseContainer
	var newscaleUpCandidateList []global.ScaleCandidateContainer

	var removeCandidateContainerList []global.CheckpointContainer

	// If the container with the completed checkpoint is paused
	// Stateless containers are removed without being checkpoint

	if global.IS_STATEFUL {
		for _, pauseContainer := range *pauseContainerList {
			shouldRemove := false
			for _, checkpointContainer := range *checkpointContainerList {
				if pauseContainer.PodName == checkpointContainer.PodName && checkpointContainer.IsCheckpoint {
					if pauseContainer.Timestamp < checkpointContainer.Timestamp+global.RECHECKPOINT_THRESHOLD {
						// check duration of pause
						checkpointContainer.ContainerData.PauseTime += time.Now().Unix() - pauseContainer.Timestamp
						removeCandidateContainerList = append(removeCandidateContainerList, checkpointContainer)
						shouldRemove = true
						break
					}
				}
			}
			if !shouldRemove {
				newPauseContainerList = append(newPauseContainerList, pauseContainer)
			}
		}

		for _, scaleContainer := range *scaleUpCandidateList {
			shouldRemove := false
			for _, removeContainer := range removeCandidateContainerList {
				if scaleContainer.PodName == removeContainer.PodName {
					shouldRemove = true
					break
				}
			}
			if !shouldRemove {
				newscaleUpCandidateList = append(newscaleUpCandidateList, scaleContainer)
			}
		}

		*pauseContainerList = newPauseContainerList
		*scaleUpCandidateList = newscaleUpCandidateList
	} else {
		count := 0
		for _, pauseContainer := range *pauseContainerList {
			if count >= 2 {
				newPauseContainerList = append(newPauseContainerList, pauseContainer)
				continue
			}

			container := global.CheckpointContainer{
				PodName:       pauseContainer.PodName,
				PodId:         pauseContainer.PodId,
				ContainerName: pauseContainer.ContainerName,
				Timestamp:     time.Now().Unix(), // current timestamp value
				PodUid:        pauseContainer.PodId,
				ContainerData: pauseContainer.ContainerData,
				ScaleSize:     pauseContainer.ScaleSize,
			}
			container.ContainerData.PauseTime += time.Now().Unix() - pauseContainer.Timestamp
			removeCandidateContainerList = append(removeCandidateContainerList, container)
			count++
		}

		for _, scaleContainer := range *scaleUpCandidateList {
			shouldRemove := false
			for _, removeContainer := range removeCandidateContainerList {
				if scaleContainer.PodName == removeContainer.PodName {
					shouldRemove = true
					break
				}
			}
			if !shouldRemove {
				newscaleUpCandidateList = append(newscaleUpCandidateList, scaleContainer)
			}
		}

		*pauseContainerList = newPauseContainerList
		*scaleUpCandidateList = newscaleUpCandidateList
	}

	return removeCandidateContainerList
}

func PerformRemoveContainers(
	client internalapi.RuntimeService,
	removeContainerToChan chan global.CheckpointContainer,
	removeCandidateContainerList []global.CheckpointContainer,
	currentRunningPods []string) []string {

	for _, removeCandidate := range removeCandidateContainerList {
		RemoveContainer(client, removeCandidate.PodName)
		currentRunningPods = RemovePodFromList(currentRunningPods, removeCandidate.PodName)
		removeCandidate.CheckpointData.RemoveStartTime = time.Now().Unix()
		//removeCandidate.CheckpointData.MemoryLimitInBytes
		removeCandidate.ContainerData.NumOfRemove++
		if removeCandidate.ContainerData.Attempt > 0 {
			removeCandidate.ContainerData.PastAttempt = removeCandidate.ContainerData.Attempt
		}
		removeContainerToChan <- removeCandidate
	}
	return currentRunningPods
}

func RemovePodFromList(currentRunningPods []string, podName string) []string {
	for i, name := range currentRunningPods {
		if name == podName {
			return append(currentRunningPods[:i], currentRunningPods[i+1:]...)
		}
	}
	return currentRunningPods
}

func RemoveContainer(client internalapi.RuntimeService, podName string) {

	command1 := `container_id=$(sudo crictl ps | grep "` + podName + `" | awk '{print $1}') &&
	[ -n "$container_id" ] &&
	pid=$(sudo crictl inspect "$container_id" | grep '"pid":' | awk -F': ' '{print $2}' | sed 's/[^0-9]//g') &&
	[ -n "$pid" ] &&
	sudo kill -9 "$pid" &&
	kubectl delete pods ` + podName + ` --grace-period=0 --force`
	//command2 := "container_id=$(sudo crictl ps | grep " + podName + " | awk '{print $1}') && [ -n \"$container_id\" ] && echo $container_id | xargs sudo crictl stop && echo $container_id | xargs sudo crictl rm && kubectl delete pods " + podName + " --grace-period=0 --force"

	output, err := exec.Command("bash", "-c", command1).Output()
	fmt.Println(err)
	fmt.Println((string(output)))
	/*
		for {
			var output []byte
			var err error

			if noErr {
				output, err := exec.Command("bash", "-c", command1).Output()
			} else {
				output, err = exec.Command("bash", "-c", command1).Output()
			}

			if err != nil {
				fmt.Println(err)
				fmt.Println((string(output)))
				noErr = false
			} else {
				fmt.Println((string(output)))
				break
			}
		}
	*/
}

func RemoveRestartedRepairContainer(client internalapi.RuntimeService, podName string) {

	command := "kubectl delete pods " + podName + " --grace-period=0 --force"

	for {
		output, err := exec.Command("bash", "-c", command).Output()

		if err != nil {
			fmt.Println(err)
		} else {
			fmt.Println((string(output)))
			break
		}
	}
}
