package controller

import (
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	infrav1 "github.com/affinity/distriscrape/api/v1"
)

const (
	workerAppName       = "distriscrape-worker"
	workerContainerName = "worker"
	workerImage         = "distriscrape-worker:latest"
	workerPort          = 7946
	raftDataMountPath   = "/data/raft"
	targetsConfigKey    = "targets.yaml"
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
			Ports: []corev1.ServicePort{
				{
					Name:       "peer",
					Port:       workerPort,
					TargetPort: intstr.FromInt32(workerPort),
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
			Selector: &metav1.LabelSelector{
				MatchLabels: podLabels,
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: podLabels,
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
								{Name: "SYNC_INTERVAL", Value: syncInterval},
								{Name: "WEBHOOK_URL", Value: scraper.Spec.WebhookDestination},
							},
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      "raft-data",
									MountPath: raftDataMountPath,
								},
							},
						},
					},
					Volumes: []corev1.Volume{
						{
							Name: "raft-data",
							VolumeSource: corev1.VolumeSource{
								EmptyDir: &corev1.EmptyDirVolumeSource{},
							},
						},
					},
				},
			},
		},
	}
}
