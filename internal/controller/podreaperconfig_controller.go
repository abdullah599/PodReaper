/*
Copyright 2025 abdullah599.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"slices"

	podreapercomv1alpha1 "github.com/abdullah599/PodReaper/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// PodReaperConfigReconciler reconciles a PodReaperConfig object
type PodReaperConfigReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=podreaper.com.podreaper.com,resources=podreaperconfigs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=podreaper.com.podreaper.com,resources=podreaperconfigs/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=podreaper.com.podreaper.com,resources=podreaperconfigs/finalizers,verbs=update

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the PodReaperConfig object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.20.0/pkg/reconcile
func (r *PodReaperConfigReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := log.FromContext(ctx)
	log.Info("Starting the reconciliation process")

	prc := &podreapercomv1alpha1.PodReaperConfig{}

	if err := r.Get(ctx, req.NamespacedName, prc); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	namespaces := prc.Spec.Namespaces

	log.Info("Namespaces to be watched", "namespaces", namespaces)

	// for namespace we get all the pods and if they are orphan we delete them
	for _, namespace := range namespaces {
		pods := &corev1.PodList{}
		r.List(ctx, pods, client.InNamespace(namespace))

		for _, pod := range pods.Items {
			if pod.OwnerReferences == nil {
				log.Info("Deleting pod", "pod", pod.GetName())
				r.Delete(ctx, &pod)
			}
		}
	}

	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *PodReaperConfigReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&podreapercomv1alpha1.PodReaperConfig{}).
		Watches(
			&corev1.Pod{},
			handler.EnqueueRequestsFromMapFunc(r.findObjectsForPod),
		).
		Named("podreaperconfig").
		Complete(r)
}

func (r *PodReaperConfigReconciler) findObjectsForPod(ctx context.Context, pod client.Object) []reconcile.Request {

	configs := &podreapercomv1alpha1.PodReaperConfigList{}
	r.List(ctx, configs)

	for _, config := range configs.Items {
		if slices.Contains(config.Spec.Namespaces, pod.GetNamespace()) {
			return []reconcile.Request{
				{
					NamespacedName: types.NamespacedName{
						Name:      config.GetName(),
						Namespace: config.GetNamespace(),
					},
				},
			}
		}
	}
	return nil
}
