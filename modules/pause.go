package modules

import (
	"fmt"
	"os/exec"
	"time"

	global "elastic/modules/global"
)

func CheckToPauseContainer(container global.ScaleCandidateContainer, pauseContainerList []global.PauseContainer) bool {
	for _, pauseContainer := range pauseContainerList {
		if pauseContainer.PodName == container.PodName {
			return false
		}
	}
	return true
}

func AppendPauseContainerList(pauseContainerList []global.PauseContainer, containerList []global.ScaleCandidateContainer) []global.PauseContainer {

	for _, container := range containerList {
		var pauseContainer global.PauseContainer

		pauseContainer.PodName = container.PodName
		pauseContainer.PodId = container.PodId
		pauseContainer.PodUid = container.PodUid
		pauseContainer.ContainerName = container.ContainerName
		pauseContainer.ContainerId = container.ContainerId
		pauseContainer.ContainerData = container.ContainerData
		pauseContainer.Timestamp = time.Now().Unix()
		pauseContainer.ScaleSize = container.ScaleSize

		pauseContainerList = append(pauseContainerList, pauseContainer)

	}
	return pauseContainerList
}

func PauseContainer(podUid string, continueCandicate *global.ContainerData) {
	fmt.Println("Pause")
	convertId, _ := ConvertToUUID(podUid)

	command := `echo 1 | sudo tee /sys/fs/cgroup/kubepods.slice/kubepods-burstable.slice/kubepods-burstable-pod` + convertId + `.slice/cgroup.freeze`

	_, err := exec.Command("bash", "-c", command).Output()
	if err != nil {
		fmt.Println(err)
	}
}

func ContinueContainer(podUid string, continueCandicate *global.ContainerData) {
	fmt.Println("Continue")
	covertId, _ := ConvertToUUID(podUid)

	command := `echo 0 | sudo tee /sys/fs/cgroup/kubepods.slice/kubepods-burstable.slice/kubepods-burstable-pod` + covertId + `.slice/cgroup.freeze`

	_, err := exec.Command("bash", "-c", command).Output()
	if err != nil {
		fmt.Println(err)
	}
}
