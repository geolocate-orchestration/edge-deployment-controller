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
	"github.com/matoous/go-nanoid/v2"
	apps "k8s.io/api/apps/v1"
	core "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	edgev1 "github.com/aida-dos/edge-controller/api/v1"
)

// DeploymentReconciler reconciles a Deployment object
type DeploymentReconciler struct {
	client.Client
	Scheme *runtime.Scheme

	Recorder record.EventRecorder
}

// +kubebuilder:rbac:groups=edge.aida.io,resources=deployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=edge.aida.io,resources=deployments/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;delete
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch

func (r *DeploymentReconciler) Reconcile(req ctrl.Request) (ctrl.Result, error) {
	ctx := context.Background()
	id, _ := gonanoid.New(5)

	klog.Infoln(id, "- fetching 'Deployment' resource")
	edgeDeployment := edgev1.Deployment{}
	if err := r.Client.Get(ctx, req.NamespacedName, &edgeDeployment); err != nil {
		klog.Errorln(id, err)
		// Ignore NotFound errors as they will be retried automatically if the
		// resource is created in future.
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	return r.handleNewOrUpdate(ctx, &edgeDeployment, id)
}

func (r *DeploymentReconciler) handleNewOrUpdate(
	ctx context.Context, edgeDeployment *edgev1.Deployment, id string) (ctrl.Result, error) {

	// TODO: delete out of date deployments

	// Setup edgeDeployment to use aida-scheduler
	edgeDeployment.Spec.Template.Spec.SchedulerName = "aida-scheduler"

	deployment := apps.Deployment{}
	err := r.getOrCreateDeployment(ctx, edgeDeployment, &deployment, id)
	if err != nil {
		klog.Errorln(id, err)
		return ctrl.Result{}, err
	}

	updatedDeployment, err := r.syncingReplicaCount(ctx, edgeDeployment, &deployment, id)
	if err != nil {
		klog.Errorln(id, err)
		return ctrl.Result{}, err
	}

	if updatedDeployment {
		klog.Infoln(id, "updating 'Deployment' resource status")
		edgeDeployment.Status.ReadyReplicas = deployment.Status.ReadyReplicas

		newDeployment := apps.Deployment{}
		newDeployment.Name = deployment.Name
		newDeployment.Namespace = deployment.Namespace
		newDeployment.Status = *deployment.Status.DeepCopy()

		if err = r.Client.Status().Update(ctx, &newDeployment); err != nil {
			klog.Errorln(id, err)
			return ctrl.Result{}, err
		}

		klog.Infoln(id, "resource status synced")
	} else {
		klog.Infoln(id, "nothing to update, skipping")
	}

	return ctrl.Result{}, nil
}

func (r *DeploymentReconciler) getOrCreateDeployment(
	ctx context.Context, edgeDeployment *edgev1.Deployment, deployment *apps.Deployment, id string) error {

	err := r.Client.Get(ctx, client.ObjectKey{
		Namespace: edgeDeployment.Namespace, Name: edgeDeployment.Name,
	}, deployment)

	if apierrors.IsNotFound(err) {
		klog.Infoln(id, "could not find existing 'Deployment' resources, creating...")

		*deployment = *buildDeployment(*edgeDeployment)
		if err := r.Client.Create(ctx, deployment); err != nil {
			return err
		}

		r.Recorder.Eventf(edgeDeployment, core.EventTypeNormal,
			"Created", "Created deployment %q", deployment.Name)
		klog.Infoln(id, "created 'Deployment' resource")
		return nil
	} else {
		klog.Infoln(id, "existing 'Deployment' resource already exists")
	}

	if err != nil {
		return err
	}

	return nil
}

func (r *DeploymentReconciler) syncingReplicaCount(
	ctx context.Context, edgeDeployment *edgev1.Deployment, deployment *apps.Deployment, id string) (bool, error) {

	expectedReplicas := int32(1)
	if edgeDeployment.Spec.Replicas != nil {
		expectedReplicas = *edgeDeployment.Spec.Replicas
	}
	if *deployment.Spec.Replicas != expectedReplicas {
		klog.Infoln(id, "updating replica count - old_count:", *deployment.Spec.Replicas,
			", new_count:", expectedReplicas)

		deployment.Spec.Replicas = &expectedReplicas
		if err := r.Client.Update(ctx, deployment); err != nil {
			return false, err
		}

		r.Recorder.Eventf(edgeDeployment, core.EventTypeNormal, "Scaled",
			"Scaled deployment %q to %d replicas", deployment.Name, expectedReplicas)

		return true, nil
	}

	klog.Infoln(id, "replica count up to date - replica_count:", *deployment.Spec.Replicas)

	return false, nil
}

// cleanupOwnedResources will Delete any existing Deployment resources that
// were created for the given Deployment
func (r *DeploymentReconciler) cleanupResources(
	ctx context.Context, edgeDeployment *edgev1.Deployment, id string) (ctrl.Result, error) {

	// List all deployment resources owned by this Deployment
	var deployments apps.DeploymentList
	if err := r.List(
		ctx, &deployments, client.InNamespace(edgeDeployment.Namespace),
		client.MatchingFields{".metadata.controller": edgeDeployment.Name},
	); err != nil {
		klog.Errorln(id, err)
		return ctrl.Result{}, err
	}

	deleted := 0
	for _, depl := range deployments.Items {
		if err := r.Client.Delete(ctx, &depl); err != nil {
			klog.Errorln(id, err)
			return ctrl.Result{}, err
		}

		r.Recorder.Eventf(edgeDeployment, core.EventTypeNormal, "Deleted", "Deleted deployment %q", depl.Name)
		deleted++
	}

	if deleted != 0 {
		klog.Infoln(id, "finished cleaning up old 'Deployment' resources - number_deleted:", deleted)
	}

	if err := r.Client.Delete(ctx, edgeDeployment); err != nil {
		klog.Errorln(id, err)
		return ctrl.Result{}, err
	}

	klog.Infoln(id, edgeDeployment.Name, "'Deployment' deleted")

	return ctrl.Result{}, nil
}

func buildDeployment(edgeDeployment edgev1.Deployment) *apps.Deployment {
	deployment := apps.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      edgeDeployment.Name,
			Namespace: edgeDeployment.Namespace,
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(&edgeDeployment, edgev1.GroupVersion.WithKind("Deployment")),
			},
		},
		Spec: apps.DeploymentSpec{
			Replicas: edgeDeployment.Spec.Replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"deployment.edge.aida.io/deployment-name": edgeDeployment.Name,
				},
			},
			Template: core.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"deployment.edge.aida.io/deployment-name": edgeDeployment.Name,
					},
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
