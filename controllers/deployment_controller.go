/*


Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language                                                                                                                  verning permissions and
limitations under the License.
*/

package controllers

import (
	"context"
	apps "k8s.io/api/apps/v1"
	core "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"strings"

	edgev1 "github.com/mv-orchestration/edge-deployment-controller/api/v1"
	"github.com/mv-orchestration/scheduler/labels"
)

// DeploymentReconciler reconciles a Deployment object
type DeploymentReconciler struct {
	client.Client
	Scheme *runtime.Scheme

	Recorder record.EventRecorder
}

// +kubebuilder:rbac:groups=edge.mv.io,resources=deployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=edge.mv.io,resources=deployments/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;delete
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch

func (r *DeploymentReconciler) Reconcile(req ctrl.Request) (ctrl.Result, error) {
	ctx := context.Background()
	klog.Infoln("- fetching 'Deployment' resource")
	edgeDeployment := edgev1.Deployment{
		ObjectMeta: ctrl.ObjectMeta{
			Name:      req.Name,
			Namespace: req.Namespace,
		},
	}

	if err := r.Client.Get(ctx, req.NamespacedName, &edgeDeployment); err != nil {
		_, _ = r.cleanupResources(ctx, &edgeDeployment)
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	return r.handleNewOrUpdate(ctx, &edgeDeployment)
}

func (r *DeploymentReconciler) handleNewOrUpdate(
	ctx context.Context, edgeDeployment *edgev1.Deployment) (ctrl.Result, error) {

	// Setup edgeDeployment to use k8s-scheduler
	edgeDeployment.Spec.Template.Spec.SchedulerName = "k8s-scheduler"

	deployment := apps.Deployment{}
	err := r.getOrCreateDeployment(ctx, edgeDeployment, &deployment)
	if err != nil {
		klog.Errorln(err)
		return ctrl.Result{}, err
	}

	updatedDeployment, err := r.syncingReplicaCount(ctx, edgeDeployment, &deployment)
	if err != nil {
		klog.Errorln(err)
		return ctrl.Result{}, err
	}

	if updatedDeployment {
		klog.Infoln("updating 'Deployment' resource status")
		edgeDeployment.Status.ReadyReplicas = deployment.Status.ReadyReplicas

		newDeployment := apps.Deployment{}
		newDeployment.Name = deployment.Name
		newDeployment.Namespace = deployment.Namespace
		newDeployment.Status = *deployment.Status.DeepCopy()

		if err = r.Client.Status().Update(ctx, &newDeployment); err != nil {
			klog.Errorln(err)
			return ctrl.Result{}, err
		}

		klog.Infoln("resource status synced")
	} else {
		klog.Infoln("nothing to update, skipping")
	}

	return ctrl.Result{}, nil
}

func (r *DeploymentReconciler) getOrCreateDeployment(
	ctx context.Context, edgeDeployment *edgev1.Deployment, deployment *apps.Deployment) error {

	err := r.Client.Get(ctx, client.ObjectKey{
		Namespace: edgeDeployment.Namespace, Name: edgeDeployment.Name,
	}, deployment)

	if apierrors.IsNotFound(err) {
		klog.Infoln("could not find existing 'Deployment' resources, creating...")

		*deployment = *buildDeployment(edgeDeployment)
		if err := r.Client.Create(ctx, deployment); err != nil {
			return err
		}

		r.Recorder.Eventf(edgeDeployment, core.EventTypeNormal,
			"Created", "Created deployment %q", deployment.Name)
		klog.Infoln("created 'Deployment' resource")
		return nil
	} else {
		klog.Infoln("existing 'Deployment' resource already exists")
	}

	if err != nil {
		return err
	}

	return nil
}

func (r *DeploymentReconciler) syncingReplicaCount(
	ctx context.Context, edgeDeployment *edgev1.Deployment, deployment *apps.Deployment) (bool, error) {

	expectedReplicas := int32(1)
	if edgeDeployment.Spec.Replicas != nil {
		expectedReplicas = *edgeDeployment.Spec.Replicas
	}
	if *deployment.Spec.Replicas != expectedReplicas {
		klog.Infoln("updating replica count - old_count:", *deployment.Spec.Replicas,
			", new_count:", expectedReplicas)

		deployment.Spec.Replicas = &expectedReplicas
		if err := r.Client.Update(ctx, deployment); err != nil {
			return false, err
		}

		r.Recorder.Eventf(edgeDeployment, core.EventTypeNormal, "Scaled",
			"Scaled deployment %q to %d replicas", deployment.Name, expectedReplicas)

		return true, nil
	}

	klog.Infoln("replica count up to date - replica_count:", *deployment.Spec.Replicas)

	return false, nil
}

// cleanupOwnedResources will Delete any existing Deployment resources that
// were created for the given Deployment
func (r *DeploymentReconciler) cleanupResources(
	ctx context.Context, edgeDeployment *edgev1.Deployment) (ctrl.Result, error) {

	deployment := &apps.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      edgeDeployment.Name,
			Namespace: edgeDeployment.Namespace,
		},
	}

	if err := r.Client.Delete(ctx, deployment); err != nil {
		klog.Errorln(err)
		return ctrl.Result{}, err
	}

	klog.Infoln(deployment.Name, "'Deployment' deleted")

	return ctrl.Result{}, nil
}

func hasAnyRequiredLocation(edgeDeployment *edgev1.Deployment) bool {
	return len(edgeDeployment.Spec.RequiredGeographicalLocation.Continents) > 0 ||
		len(edgeDeployment.Spec.RequiredGeographicalLocation.Countries) > 0 ||
		len(edgeDeployment.Spec.RequiredGeographicalLocation.Cities) > 0
}

func hasAnyPreferredLocation(edgeDeployment *edgev1.Deployment) bool {
	return len(edgeDeployment.Spec.PreferredGeographicalLocation.Continents) > 0 ||
		len(edgeDeployment.Spec.PreferredGeographicalLocation.Countries) > 0 ||
		len(edgeDeployment.Spec.PreferredGeographicalLocation.Cities) > 0
}

func buildDeployment(edgeDeployment *edgev1.Deployment) *apps.Deployment {
	lbs := map[string]string{
		"deployment.edge.mv.io/deployment-name": edgeDeployment.Name,
	}

	if hasAnyRequiredLocation(edgeDeployment) {
		lbs[labels.WorkloadRequiredLocation] =
			strings.Join(edgeDeployment.Spec.RequiredGeographicalLocation.Cities, "_") + "-" +
				strings.Join(edgeDeployment.Spec.RequiredGeographicalLocation.Countries, "_") + "-" +
				strings.Join(edgeDeployment.Spec.RequiredGeographicalLocation.Continents, "_")
	} else if hasAnyPreferredLocation(edgeDeployment) {
		lbs[labels.WorkloadPreferredLocation] =
			strings.Join(edgeDeployment.Spec.PreferredGeographicalLocation.Cities, "_") + "-" +
				strings.Join(edgeDeployment.Spec.PreferredGeographicalLocation.Countries, "_") + "-" +
				strings.Join(edgeDeployment.Spec.PreferredGeographicalLocation.Continents, "_")
	}

	deployment := apps.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      edgeDeployment.Name,
			Namespace: edgeDeployment.Namespace,
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(edgeDeployment, edgev1.GroupVersion.WithKind("Deployment")),
			},
		},
		Spec: apps.DeploymentSpec{
			Replicas: edgeDeployment.Spec.Replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"deployment.edge.mv.io/deployment-name": edgeDeployment.Name,
				},
			},
			Template: core.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: lbs,
				},
				Spec: edgeDeployment.Spec.Template.Spec,
			},
		},
	}
	return &deployment
}

func (r *DeploymentReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if err := mgr.GetFieldIndexer().IndexField(
		&apps.Deployment{}, ".metadata.controller",
		func(rawObj runtime.Object) []string {
			// grab the Deployment object, extract the owner...
			depl := rawObj.(*apps.Deployment)
			owner := metav1.GetControllerOf(depl)
			if owner == nil {
				return nil
			}
			// ...make sure it's a Deployment...
			if owner.APIVersion != edgev1.GroupVersion.String() || owner.Kind != "Deployment" {
				return nil
			}

			// ...and if so, return it
			return []string{owner.Name}
		},
	); err != nil {
		return err
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&edgev1.Deployment{}).
		Owns(&apps.Deployment{}).
		Complete(r)
}
