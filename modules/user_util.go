package modules

import (
	global "elastic/modules/global"
	"fmt"
	"os/exec"
	"strings"
)

func GetmemoryUsagePercents(podInfoSet []global.PodData) []global.PodData {
	// get current container memory usage and limit value
	for i := 0; i < len(podInfoSet); i++ {
		containerMemoryUsages := podInfoSet[i].Container[0].Resource[0].MemoryUsageBytes
		// if limit is not set, it will appear as 0; if set, it will output normally.
		containerMemoryLimits := podInfoSet[i].Container[0].Cgroup.MemoryLimitInBytes

		// exception handling
		// container without limit set, not burstable container
		if containerMemoryLimits == 0 {
			podInfoSet[i].Container[0].Resource[0].ConMemUtil = 0
		}
		podInfoSet[i].Container[0].Resource[0].ConMemUtil = float64(containerMemoryUsages) / float64(containerMemoryLimits)
	}

	return podInfoSet
}

func SelectRestrictContainers(podInfoSet []global.PodData, selectContainerId []string) (float64, int32, []string) {

	selectMemoryUsagePercents := 100.00
	indexOfSelectContainers := 0

	for i := 0; i < len(podInfoSet); i++ {
		// container without limit set, not burstable container (exception handling)
		// not lowest memory usage percents
		if podInfoSet[i].Container[0].Resource[0].ConMemUtil == 0 ||
			selectMemoryUsagePercents < podInfoSet[i].Container[0].Resource[0].ConMemUtil {
			continue
		}

		// verify if already restricted the container resources
		isOverlap := false
		for j := 0; j < len(selectContainerId); j++ {
			if podInfoSet[i].Container[0].Id == selectContainerId[j] {
				isOverlap = true
				break
			}
		}
		if isOverlap == true {
			continue
		}

		// choose lowest memory usage percents
		selectMemoryUsagePercents = podInfoSet[i].Container[0].Resource[0].ConMemUtil
		indexOfSelectContainers = i
	}

	return selectMemoryUsagePercents, int32(indexOfSelectContainers), selectContainerId
}

func ConvertToUUID(input string) (string, error) {
	// Exception handling
	if len(input) != 36 {
		return "", fmt.Errorf("The input value must be 36 digits.")
	}

	// 하이픈을 언더스코어로 변환
	return strings.Replace(input, "-", "_", -1), nil
}

func RemovePodofPodInfoSet(podInfoSet []global.PodData, i int) []global.PodData {
	podInfoSet[i] = podInfoSet[len(podInfoSet)-1]
	return podInfoSet[:len(podInfoSet)-1]
}

func GetContainerFreeze(podUid string) string {
	covertId, _ := ConvertToUUID(podUid)
	command := `sudo cat /sys/fs/cgroup/kubepods.slice/kubepods-burstable.slice/kubepods-burstable-pod` + covertId + `.slice/cgroup.freeze`
	output, _ := exec.Command("bash", "-c", command).Output()

	return strings.TrimSpace(string(output))
}
