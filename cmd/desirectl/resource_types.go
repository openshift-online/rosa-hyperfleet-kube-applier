package main

import (
	"fmt"
	"strings"

	kubeapplier "github.com/rrp-bot/rosa-hyperfleet-kube-applier/api/kubeapplier"
)

type ResourceInfo struct {
	Group    string
	Version  string
	Resource string
	Kind     string
	Aliases  []string
}

var knownResources = []ResourceInfo{
	{Group: "", Version: "v1", Resource: "configmaps", Kind: "ConfigMap", Aliases: []string{"cm"}},
	{Group: "", Version: "v1", Resource: "secrets", Kind: "Secret"},
	{Group: "", Version: "v1", Resource: "namespaces", Kind: "Namespace", Aliases: []string{"ns"}},
	{Group: "", Version: "v1", Resource: "services", Kind: "Service", Aliases: []string{"svc"}},
	{Group: "", Version: "v1", Resource: "serviceaccounts", Kind: "ServiceAccount", Aliases: []string{"sa"}},
	{Group: "", Version: "v1", Resource: "pods", Kind: "Pod", Aliases: []string{"po"}},
	{Group: "apps", Version: "v1", Resource: "deployments", Kind: "Deployment", Aliases: []string{"deploy", "dep"}},
	{Group: "apps", Version: "v1", Resource: "statefulsets", Kind: "StatefulSet", Aliases: []string{"sts"}},
	{Group: "apps", Version: "v1", Resource: "daemonsets", Kind: "DaemonSet", Aliases: []string{"ds"}},
	{Group: "batch", Version: "v1", Resource: "jobs", Kind: "Job"},
	{Group: "batch", Version: "v1", Resource: "cronjobs", Kind: "CronJob", Aliases: []string{"cj"}},
	{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterroles", Kind: "ClusterRole"},
	{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterrolebindings", Kind: "ClusterRoleBinding"},
	{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "roles", Kind: "Role"},
	{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "rolebindings", Kind: "RoleBinding"},
}

func resolveResourceType(input string) (*ResourceInfo, error) {
	lower := strings.ToLower(input)
	for i := range knownResources {
		r := &knownResources[i]
		if strings.ToLower(r.Resource) == lower || strings.ToLower(r.Kind) == lower {
			return r, nil
		}
		for _, alias := range r.Aliases {
			if alias == lower {
				return r, nil
			}
		}
	}
	return nil, fmt.Errorf("unknown resource type %q", input)
}

func resourceTypeFromManifest(_ string, kind string) (*ResourceInfo, error) {
	for i := range knownResources {
		r := &knownResources[i]
		if strings.EqualFold(r.Kind, kind) {
			return r, nil
		}
	}
	return nil, fmt.Errorf("unknown kind %q", kind)
}

func formatResourceName(info *ResourceInfo, name string) string {
	if info.Group != "" {
		return fmt.Sprintf("%s.%s/%s", strings.TrimSuffix(info.Resource, "s"), info.Group, name)
	}
	singular := strings.TrimSuffix(info.Resource, "s")
	return fmt.Sprintf("%s/%s", singular, name)
}

func resourceRefFromGVR(info *ResourceInfo, namespace, name string) kubeapplier.ResourceReference {
	return kubeapplier.ResourceReference{
		Group:     info.Group,
		Version:   info.Version,
		Resource:  info.Resource,
		Namespace: namespace,
		Name:      name,
	}
}
