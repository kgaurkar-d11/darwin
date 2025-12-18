package helm_utils

import (
	"compute/cluster_manager/utils/rest_errors"
	"fmt"
	"os"

	"helm.sh/helm/v3/pkg/action"
	"helm.sh/helm/v3/pkg/kube"
)

func GetActionConfig(kubeConfigPath string, namespace string) (*action.Configuration, rest_errors.RestErr) {
	// Use in-cluster config if running in Kubernetes, otherwise use provided kubeconfig
	effectiveKubeConfigPath := getEffectiveKubeConfigPath(kubeConfigPath)

	actionConfig := new(action.Configuration)
	if err := actionConfig.Init(kube.GetConfig(effectiveKubeConfigPath, "", namespace), namespace, os.Getenv("HELM_DRIVER"), func(format string, v ...interface{}) {
		_ = fmt.Sprintf(format, v)
	}); err != nil {
		restError := rest_errors.NewInternalServerError("Error initiating the action config", err)
		return nil, restError
	}
	return actionConfig, nil
}
