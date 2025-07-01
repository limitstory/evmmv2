package modules

const ENDPOINT string = "unix:///var/run/crio/crio.sock"
const NODENAME string = "limitstory-virtualbox"

const IS_STATEFUL bool = true

const NUM_OF_WORKERS int64 = 3

const MAX_VALUE int = 10000
const MIN_VALUE int = -10000

const DEFAULT_CPU_QUOTA int64 = 20000

const CORES_TO_MILLICORES float64 = 1000.0
const NANOCORES_TO_MILLICORES int64 = 1000000
const NANOSECONDS int64 = 1000000000

const MAX_TIME_WINDOW int64 = 60
const SCALE_DOWN_THRESHOLD int = 15

const CONTAINER_MEMORY_SLO_UPPER float64 = 0.80
const CONTAINER_MEMORY_SLO float64 = 0.75
const CONTAINER_MEMORY_SLO_LOWER float64 = 0.70

const CHECKPOINT_THRESHOLD float64 = 0.82
const RECHECKPOINT_THRESHOLD int64 = 120

const START_THRESHOLD int64 = 60

const MAX_MEMORY_USAGE_THRESHOLD float64 = 0.95
const MAX_MEMORY_USAGE_THRESHOLD2 float64 = 0.95

const CREATE_IMAGE_THRESHOLD float64 = 0.90
const MAX_REPAIR_MEMORY_USAGE_THRESHOLD float64 = 0.80

const SACLE_WEIGHT = 60

const CONTAINER_MEMORY_USAGE_THRESHOLD float64 = 0.90

const MIN_SIZE_PER_CONTAINER int64 = 900 * 1048576  // 1Mibibyte = 1024*1024 = 1048576
const MAX_SIZE_PER_CONTAINER int64 = 1300 * 1048576 // 1Mibibyte = 1024*1024 = 1048576

const MIN_SCALE_SIZE int64 = 100 * 1048576 // 1Mibibyte = 1024*1024 = 1048576

const TIMEOUT_INTERVAL int32 = 3

// AvgConTotalMemUtil, AvgNodeMemUtil, VarConMemUtil, AvgCpuUtil
var RESOURCE_VECTOR = []float64{1, 1, 1, 1}

// Resource, PriorityID, Penalty, Reward
var PRIORITY_VECTOR = []float64{1, 1, 1, 1}

var NumOfTotalScale int64 = 0

type CheckpointTime struct {
	PodName        string
	CheckpointTime int64
}

type ImageTime struct {
	PodName   string
	ImageTime int64
}

type RemoveTime struct {
	PodName    string
	RemoveTime int64
}

type RepairTime struct {
	PodName    string
	RepairTime int64
}

type PriorityContainer struct {
	PodName       string
	ContainerName string
	ContainerId   string
	Priority      float64
}

type ScaleCandidateContainer struct {
	PodName       string
	PodId         string
	PodUid        string
	ContainerName string
	ContainerId   string
	ContainerData *ContainerData
	ScaleSize     int64
}

type PauseContainer struct {
	PodName        string
	PodId          string
	PodUid         string
	ContainerName  string
	ContainerId    string
	Timestamp      int64
	IsCheckpoint   bool
	ContainerData  *ContainerData
	CheckpointData CheckpointMetaData
	ScaleSize      int64
}

type CheckpointContainer struct {
	PodName               string
	PodId                 string
	PodUid                string
	ContainerName         string
	ContainerId           string
	Timestamp             int64
	DuringCheckpoint      bool
	AbortedCheckpoint     bool
	IsCheckpoint          bool
	DuringCreateImages    bool
	CreateImages          bool
	DuringCreateContainer bool
	CreateContainer       bool
	StartCheckpointTime   int64
	EndCheckpointTime     int64
	StartRemoveTime       int64
	EndREmoveTime         int64
	StartImageTime        int64
	EndImageTime          int64
	StartRepairTime       int64
	EndRepairTime         int64
	ContainerData         *ContainerData
	CheckpointData        CheckpointMetaData
	ScaleSize             int64
}

type RepairContainer struct {
	PodName               string
	PodId                 string
	PodUid                string
	ContainerName         string
	ContainerId           string
	DuringCheckpoint      bool
	IsCheckpoint          bool
	DuringCreateContainer bool
	CreatingContainer     bool
	CreateContainer       bool
	ContainerData         *ContainerData
	CheckpointData        CheckpointMetaData
	ScaleSize             int64
}

type CheckpointMetaData struct {
	CheckpointName     string
	MemoryLimitInBytes int64
	RemoveStartTime    int64
	RemoveEndTime      int64
}
