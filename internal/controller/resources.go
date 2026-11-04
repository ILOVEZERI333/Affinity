package controller

import (
	"fmt"
	"strconv"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	infrav1 "github.com/affinity/distriscrape/api/v1"
)

const (
	workerAppName       = "distriscrape-worker"
	workerContainerName = "worker"
	workerImage         = "distriscrape-worker:latest"
	workerPort          = 7946
	workerAPIPort       = 7947 // peer API (join/handoff); Raft's port + 1
	workerHealthPort    = 8080 // liveness probe (/healthz)
	raftDataVolumeName  = "raft-data"
	raftDataMountPath   = "/data/raft"
	raftDataStorageSize = "1Gi"
	targetsConfigKey    = "targets.yaml"
	targetsVolumeName   = "targets"
	targetsMountPath    = "/etc/affinity"
	defaultSyncInterval = "30s"

	ConditionWorkersReady = "WorkersReady"
)

func labelsFor(scraper *infrav1.DistributedScraper) map[string]string {
	return map[string]string{
		"app.kubernetes.io/name":     workerAppName,
		"app.kubernetes.io/instance": scraper.Name,
	}
}

func configMapName(scraper *infrav1.DistributedScraper) string {
	return scraper.Name + "-targets"
}

func workersResourceName(scraper *infrav1.DistributedScraper) string {
	return scraper.Name + "-workers"
}

// peerAddresses returns the stable DNS address of every worker ordinal,
// resolvable via the headless Service once pods are scheduled. Each worker
// gets the full set (including itself) and is responsible for skipping its
// own address when joining the Raft cluster.
func peerAddresses(scraper *infrav1.DistributedScraper) []string {
	svc := workersResourceName(scraper)
	addrs := make([]string, 0, scraper.Spec.WorkerNodes)
	for i := int32(0); i < scraper.Spec.WorkerNodes; i++ {
		addrs = append(addrs, fmt.Sprintf("%s-%d.%s.%s.svc.cluster.local:%d", svc, i, svc, scraper.Namespace, workerPort))
	}
	return addrs
}

func desiredConfigMap(scraper *infrav1.DistributedScraper) *corev1.ConfigMap {
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      configMapName(scraper),
			Namespace: scraper.Namespace,
			Labels:    labelsFor(scraper),
		},
		Data: map[string]string{
			targetsConfigKey: strings.Join(scraper.Spec.TargetList, "\n") + "\n",
		},
	}
}

func desiredService(scraper *infrav1.DistributedScraper) *corev1.Service {
	name := workersResourceName(scraper)
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: scraper.Namespace,
			Labels:    labelsFor(scraper),
		},
		Spec: corev1.ServiceSpec{
			ClusterIP: "None",
			Selector:  labelsFor(scraper),
			// Pods must resolve each other (and themselves) before they are
			// Ready: Raft can't form a cluster until peers can dial each other.
			PublishNotReadyAddresses: true,
			Ports: []corev1.ServicePort{
				{
					Name:       "peer",
					Port:       workerPort,
					TargetPort: intstr.FromInt32(workerPort),
					Protocol:   corev1.ProtocolTCP,
				},
				{
					Name:       "api",
					Port:       workerAPIPort,
					TargetPort: intstr.FromInt32(workerAPIPort),
					Protocol:   corev1.ProtocolTCP,
				},
				{
					Name:       "health",
					Port:       workerHealthPort,
					TargetPort: intstr.FromInt32(workerHealthPort),
					Protocol:   corev1.ProtocolTCP,
				},
			},
		},
	}
}

func desiredStatefulSet(scraper *infrav1.DistributedScraper) *appsv1.StatefulSet {
	name := workersResourceName(scraper)
	syncInterval := scraper.Spec.SyncInterval
	if syncInterval == "" {
		syncInterval = defaultSyncInterval
	}

	podLabels := labelsFor(scraper)
	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: scraper.Namespace,
			Labels:    podLabels,
		},
		Spec: appsv1.StatefulSetSpec{
			Replicas:    &scraper.Spec.WorkerNodes,
			ServiceName: name,
			// Readiness waits for a Raft leader, which needs a quorum of pods
			// running. With the default OrderedReady policy pod N+1 would wait
			// for pod N to be Ready and a cold start could never form one.
			// (Immutable: only applies to StatefulSets created after this.)
			PodManagementPolicy: appsv1.ParallelPodManagement,
			Selector: &metav1.LabelSelector{
				MatchLabels: podLabels,
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: podLabels,
					// The de-facto convention for Prometheus pod discovery.
					Annotations: map[string]string{
						"prometheus.io/scrape": "true",
						"prometheus.io/port":   strconv.Itoa(workerHealthPort),
						"prometheus.io/path":   "/metrics",
					},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:            workerContainerName,
							Image:           workerImage,
							ImagePullPolicy: corev1.PullIfNotPresent,
							Env: []corev1.EnvVar{
								{
									Name: "POD_NAME",
									ValueFrom: &corev1.EnvVarSource{
										FieldRef: &corev1.ObjectFieldSelector{
											FieldPath: "metadata.name",
										},
									},
								},
								{
									Name: "NODE_ID",
									ValueFrom: &corev1.EnvVarSource{
										FieldRef: &corev1.ObjectFieldSelector{
											FieldPath: "metadata.name",
										},
									},
								},
								{Name: "BIND_ADDR", Value: ":7946"},
								{Name: "DATA_DIR", Value: raftDataMountPath},
								{Name: "TARGETS_FILE", Value: targetsMountPath + "/" + targetsConfigKey},
								{Name: "SYNC_INTERVAL", Value: syncInterval},
								{Name: "WEBHOOK_URL", Value: scraper.Spec.WebhookDestination},
								{Name: "PEER_ADDRS", Value: strings.Join(peerAddresses(scraper), ",")},
							},
							// Process-level liveness only (see worker.LivenessHandler):
							// a dead or wedged process gets replaced; Raft state is
							// deliberately not consulted.
							LivenessProbe: &corev1.Probe{
								ProbeHandler: corev1.ProbeHandler{
									HTTPGet: &corev1.HTTPGetAction{
										Path: "/healthz",
										Port: intstr.FromInt(workerHealthPort),
									},
								},
								InitialDelaySeconds: 5,
								PeriodSeconds:       5,
								FailureThreshold:    3,
							},
							// Readiness (unlike liveness) does look at Raft: a pod with
							// no known leader is unready, which keeps it out of
							// ReadyWorkers without getting it restarted.
							ReadinessProbe: &corev1.Probe{
								ProbeHandler: corev1.ProbeHandler{
									HTTPGet: &corev1.HTTPGetAction{
										Path: "/readyz",
										Port: intstr.FromInt(workerHealthPort),
									},
								},
								InitialDelaySeconds: 2,
								PeriodSeconds:       5,
								FailureThreshold:    2,
							},
							Ports: []corev1.ContainerPort{
								{Name: "health", ContainerPort: workerHealthPort, Protocol: corev1.ProtocolTCP},
							},
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      raftDataVolumeName,
									MountPath: raftDataMountPath,
								},
								{
									Name:      targetsVolumeName,
									MountPath: targetsMountPath,
									ReadOnly:  true,
								},
							},
						},
					},
					// The targets ConfigMap is mounted as a file so the leader
					// can read the target list to place. Kubelet refreshes the
					// file when the ConfigMap changes, and the placer re-reads it.
					Volumes: []corev1.Volume{
						{
							Name: targetsVolumeName,
							VolumeSource: corev1.VolumeSource{
								ConfigMap: &corev1.ConfigMapVolumeSource{
									LocalObjectReference: corev1.LocalObjectReference{Name: configMapName(scraper)},
								},
							},
						},
					},
				},
			},
			// PVC per ordinal so a restarted pod on the same ordinal reattaches
			// to its own Raft log/stable store instead of starting from empty
			// state (identity surviving restart is the Phase 4 invariant).
			VolumeClaimTemplates: []corev1.PersistentVolumeClaim{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:   raftDataVolumeName,
						Labels: podLabels,
					},
					Spec: corev1.PersistentVolumeClaimSpec{
						AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
						Resources: corev1.VolumeResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceStorage: resource.MustParse(raftDataStorageSize),
							},
						},
					},
				},
			},
		},
	}
}
