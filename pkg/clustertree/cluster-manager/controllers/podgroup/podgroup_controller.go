package podgroup

import (
	"context"
	"fmt"
	"github.com/kosmos.io/kosmos/pkg/apis/kosmos/v1alpha1"
	clustertreeutils "github.com/kosmos.io/kosmos/pkg/clustertree/cluster-manager/utils"
	leafUtils "github.com/kosmos.io/kosmos/pkg/clustertree/cluster-manager/utils"
	"github.com/kosmos.io/kosmos/pkg/utils"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const (
	PodGroupControllerName = "podgroup-controller"
)

type PodGroupReconciler struct {
	client.Client
	RootClient client.Client

	envResourceManager utils.EnvResourceManager

	GlobalLeafManager       leafUtils.LeafResourceManager
	GlobalLeafClientManager clustertreeutils.LeafClientResourceManager
}

func (r *PodGroupReconciler) Reconcile(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
	var cachepodgroup v1alpha1.PodGroup
	if err := r.Get(ctx, request.NamespacedName, &cachepodgroup); err != nil {
		if errors.IsNotFound(err) {
			// TODO: we cannot get leaf pod when we donnot known the node name of pod, so delete all ...
			nodeNames := r.GlobalLeafManager.ListNodes()
			for _, nodeName := range nodeNames {
				lr, err := r.GlobalLeafManager.GetLeafResourceByNodeName(nodeName)
				if err != nil {
					// wait for leaf resource init
					return reconcile.Result{RequeueAfter: utils.DefaultRequeueTime}, nil
				}
				if err := r.DeletePodInLeafCluster(ctx, lr, request.NamespacedName, false); err != nil {
					klog.Errorf("delete pod in leaf error[1]: %v,  %s", err, request.NamespacedName)
					return reconcile.Result{RequeueAfter: utils.DefaultRequeueTime}, nil
				}
			}
			return reconcile.Result{}, nil
		}
		klog.Errorf("get %s error: %v", request.NamespacedName, err)
		return reconcile.Result{RequeueAfter: utils.DefaultRequeueTime}, nil
	}

	podgroup := *(cachepodgroup.DeepCopy())
	NodeName := "leaf1"

	//this  GetLeafResourceByNodeName
	lr, err := r.GlobalLeafManager.GetLeafResourceByNodeName(NodeName)
	if err != nil {
		// wait for leaf resource init
		return reconcile.Result{RequeueAfter: utils.DefaultRequeueTime}, nil
	}

	// skip namespace
	if len(lr.Namespace) > 0 && lr.Namespace != podgroup.Namespace {
		return reconcile.Result{}, nil
	}

	// delete pod in leaf
	if !podgroup.GetDeletionTimestamp().IsZero() {
		if err := r.DeletePodInLeafCluster(ctx, lr, request.NamespacedName, true); err != nil {
			klog.Errorf("delete pod in leaf error[1]: %v,  %s", err, request.NamespacedName)
			return reconcile.Result{RequeueAfter: utils.DefaultRequeueTime}, nil
		}
		return reconcile.Result{}, nil
	}

	lcr, err := r.leafClientResource(lr)
	if err != nil {
		klog.Errorf("Failed to get leaf client resource %v", lr.Cluster.Name)
		return reconcile.Result{RequeueAfter: utils.DefaultRequeueTime}, nil
	}

	leafPod := &corev1.Pod{}
	err = lcr.Client.Get(ctx, request.NamespacedName, leafPod)

	// create pod in leaf
	if err != nil {
		if errors.IsNotFound(err) {
			if err := r.CreatePodInLeafCluster(ctx, lr, &podgroup); err != nil {
				klog.Errorf("create pod inleaf error, err: %s", err)
				return reconcile.Result{RequeueAfter: utils.DefaultRequeueTime}, nil
			} else {
				return reconcile.Result{}, nil
			}
		} else {
			klog.Errorf("get pod in leaf error[3]: %v,  %s", err, request.NamespacedName)
			return reconcile.Result{RequeueAfter: utils.DefaultRequeueTime}, nil
		}
	}

	// update pod in leaf
	//if podutils.ShouldEnqueue(leafPod, &rootpod) {
	//	if err := r.UpdatePodInLeafCluster(ctx, lr, &rootpod, leafPod, r.GlobalLeafManager.GetClusterNode(rootpod.Spec.NodeName).LeafNodeSelector); err != nil {
	//		return reconcile.Result{RequeueAfter: utils.DefaultRequeueTime}, nil
	//	}
	//}

	return reconcile.Result{}, nil
}

func (r *PodGroupReconciler) DeletePodInLeafCluster(ctx context.Context, lr *leafUtils.LeafResource, name types.NamespacedName, b bool) error {
	return nil
}

func (r *PodGroupReconciler) leafClientResource(lr *leafUtils.LeafResource) (*leafUtils.LeafClientResource, error) {
	actualClusterName := leafUtils.GetActualClusterName(lr.Cluster)
	lcr, err := r.GlobalLeafClientManager.GetLeafResource(actualClusterName)
	if err != nil {
		return nil, fmt.Errorf("get leaf client resource err: %v", err)
	}
	return lcr, nil
}

func (r *PodGroupReconciler) CreatePodInLeafCluster(ctx context.Context, lr *leafUtils.LeafResource, podgroup *v1alpha1.PodGroup) error {

	newPodGroup := &v1alpha1.PodGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podgroup.Name,      // 使用主集群的 Name
			Namespace: podgroup.Namespace, // 使用主集群的 Namespace
		},
		Spec: v1alpha1.PodGroupSpec{
			MinMember: podgroup.Spec.MinMember, // 继承 MinMember 配置
			Queue:     podgroup.Spec.Queue,     // 继承 Queue 配置
		},
	}
	// 检查子集群中是否已经存在同名的 PodGroup
	existingPodGroup := &v1alpha1.PodGroup{}
	lcr, err := r.leafClientResource(lr)
	if err != nil {
		klog.Errorf("Failed to get leaf client resource %v", lr.Cluster.Name)
		return err
	}
	if err := lcr.Client.Get(ctx, client.ObjectKey{Name: newPodGroup.Name, Namespace: newPodGroup.Namespace}, existingPodGroup); err == nil {
		fmt.Printf("PodGroup %s/%s 已经存在于子集群中，跳过创建\n", newPodGroup.Namespace, newPodGroup.Name)
		return nil
	}

	// 在子集群中创建 PodGroup
	err = lcr.Client.Create(ctx, newPodGroup)
	if err != nil {
		return fmt.Errorf("could not create pod: %v", err)
	}
	klog.V(4).Infof("Create pod %v/%+v success", newPodGroup.Namespace, newPodGroup.Name)
	return nil
}
