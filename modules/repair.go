package modules

import (
	"fmt"
	"os/exec"
	"strings"
	"sync"
	"time"

	global "elastic/modules/global"

	internalapi "k8s.io/cri-api/pkg/apis"
)

func CreateImageContainer(resultChan chan global.CheckpointContainer, client internalapi.RuntimeService, systemInfoSet []global.SystemInfo, podIndex map[string]int64,
	podInfoSet []global.PodData, currentRunningPods []string, lenghOfCurrentRunningPods int, priorityMap map[string]global.PriorityContainer,
	removeContainerList []global.CheckpointContainer) {
	if global.IS_STATEFUL {
		var wg sync.WaitGroup

		sumLimitMemorySize := int64(systemInfoSet[len(systemInfoSet)-1].Memory.Used)
		if sumLimitMemorySize > int64(float64(systemInfoSet[len(systemInfoSet)-1].Memory.Total)*global.CREATE_IMAGE_THRESHOLD) {
			return
		}

		wg.Add(len(removeContainerList))
		for _, repairContainer := range removeContainerList {
			// 비동기 구현
			go func(container global.CheckpointContainer) {
				if container.DuringCreateImages || container.CreateImages || container.DuringCreateContainer || container.CreateContainer {
					return
				}
				container.DuringCreateImages = true
				resultChan <- container

				container.StartImageTime = time.Now().Unix()
				for {
					if MakeContainerFromCheckpoint(container) {
						break
					} else {
						time.Sleep(time.Second)
					}
				}
				container.EndImageTime = time.Now().Unix()

				container.DuringCreateImages = false
				container.CreateImages = true
				resultChan <- container
			}(repairContainer)
		}
		wg.Wait()
	}
}

func DecisionRepairContainer(
	resultChan chan global.CheckpointContainer,
	client internalapi.RuntimeService,
	systemInfoSet []global.SystemInfo,
	podIndex map[string]int64,
	podInfoSet []global.PodData,
	currentRunningPods []string,
	priorityMap map[string]global.PriorityContainer,
	removeContainerList []global.CheckpointContainer) {

	var wg sync.WaitGroup
	var repairContainerCandidateList []global.CheckpointContainer

	// 높은 priority 부여 필요함
	// 해당 작업이 복구 구현에서의 핵심

	if global.IS_STATEFUL {
		for _, removeContainer := range removeContainerList {
			if removeContainer.CreateImages {
				repairContainerCandidateList = append(repairContainerCandidateList, removeContainer)
			}
		}
	} else {
		for _, removeContainer := range removeContainerList {
			repairContainerCandidateList = append(repairContainerCandidateList, removeContainer)
		}
	}
	if len(repairContainerCandidateList) == 0 {
		return
	}

	wg.Add(len(repairContainerCandidateList))
	for _, repairContainerCandidate := range repairContainerCandidateList {
		defer wg.Done()
		// 비동기 구현
		go func(container global.CheckpointContainer) {
			if container.DuringCreateContainer || container.CreateContainer {
				return
			}

			container.CreateImages = false
			container.DuringCreateContainer = true
			resultChan <- container

			var repairRequestMemory int64

			if global.IS_STATEFUL {
				repairRequestMemory = container.ContainerData.Cgroup.MemoryLimitInBytes + container.ScaleSize
				if repairRequestMemory > global.MAX_SIZE_PER_CONTAINER {
					repairRequestMemory = global.MAX_SIZE_PER_CONTAINER
				}
			} else {
				repairRequestMemory = global.MIN_SIZE_PER_CONTAINER
			}

			RestoreContainer(container, repairRequestMemory)
			for {
				time.Sleep(time.Second)
				command := "kubectl get po " + container.PodName
				out, _ := exec.Command("bash", "-c", command).Output()
				strout := string(out[:])
				if !(strings.Contains(strout, "Pending")) {
					container.CheckpointData.RemoveEndTime = time.Now().Unix()
					container.StartRepairTime = time.Now().Unix()
					podInfoSet = UpdatePodData(client, container, podIndex, podInfoSet, repairRequestMemory)
					container.EndRepairTime = time.Now().Unix()
					break
				}
			}

			container.DuringCreateContainer = false
			container.CreateContainer = true

			resultChan <- container

		}(repairContainerCandidate)
	}
	wg.Wait()
}

func RestoreContainer(container global.CheckpointContainer, repairRequestMemory int64) {
	var imagePath string

	if global.IS_STATEFUL {
		imagePath = "localhost/" + container.PodName + ":latest"
	} else {
		imagePath = container.ContainerData.Image
	}

	command := fmt.Sprintf(`kubectl create -f - <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: %s
spec:
  restartPolicy: OnFailure
  priorityClassName: repair-pod
  nodeSelector:
    kubernetes.io/hostname: %s
  containers:
  - name: %s
    image: %s
    imagePullPolicy: Never
    command: ["/home/parsec-3.0/run"]
    args: ["-a", "run", "-p", "parsec.raytrace", "-i", "native"]
    resources:
      requests:
        memory: %d
      limits:
        cpu: %f
        memory: %d
EOF`, container.PodName, global.NODENAME, container.ContainerName, imagePath, repairRequestMemory,
		float64(global.DEFAULT_CPU_QUOTA)*0.00001, global.MAX_SIZE_PER_CONTAINER)

	fmt.Println(command)
	_, err := exec.Command("bash", "-c", command).Output()
	if err != nil {
		fmt.Println(err)
	}
}
