package v1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// DistributedScraperSpec defines the desired state of DistributedScraper.
type DistributedScraperSpec struct {
	// TargetList is the set of manga/manhwa identifiers to track.
	// +kubebuilder:validation:MinItems=1
	TargetList []string `json:"targetList"`

	// WorkerNodes is the desired size of the worker StatefulSet.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=1
	WorkerNodes int32 `json:"workerNodes"`

	// SyncInterval controls how often workers poll their assigned targets,
	// expressed as a Go duration string (e.g. "30s", "5m").
	// +kubebuilder:default="30s"
	SyncInterval string `json:"syncInterval,omitempty"`

	// WebhookDestination is the Discord webhook URL new chapters are posted to.
	WebhookDestination string `json:"webhookDestination"`
}

// ClusterPhase describes the lifecycle phase of the worker cluster.
type ClusterPhase string

const (
	ClusterPhasePending      ClusterPhase = "Pending"
	ClusterPhaseProvisioning ClusterPhase = "Provisioning"
	ClusterPhaseRunning      ClusterPhase = "Running"
	ClusterPhaseDegraded     ClusterPhase = "Degraded"
)

// DistributedScraperStatus defines the observed state of DistributedScraper.
type DistributedScraperStatus struct {
	// Phase summarizes the current lifecycle state of the worker cluster.
	Phase ClusterPhase `json:"phase,omitempty"`

	// LeaderNode is the pod name of the currently elected Raft leader, if known.
	LeaderNode string `json:"leaderNode,omitempty"`

	// ReadyWorkers is the number of worker pods that have joined the cluster.
	ReadyWorkers int32 `json:"readyWorkers,omitempty"`

	// LastSyncTime is the timestamp of the most recently completed scrape cycle
	// reported by any worker in the cluster.
	LastSyncTime *metav1.Time `json:"lastSyncTime,omitempty"`

	// Conditions represent the latest available observations of the resource's state.
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=".status.phase"
// +kubebuilder:printcolumn:name="Leader",type=string,JSONPath=".status.leaderNode"
// +kubebuilder:printcolumn:name="Ready",type=integer,JSONPath=".status.readyWorkers"

// DistributedScraper is the Schema for the distributedscrapers API.
type DistributedScraper struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   DistributedScraperSpec   `json:"spec,omitempty"`
	Status DistributedScraperStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// DistributedScraperList contains a list of DistributedScraper.
type DistributedScraperList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []DistributedScraper `json:"items"`
}

func init() {
	SchemeBuilder.Register(&DistributedScraper{}, &DistributedScraperList{})
}
