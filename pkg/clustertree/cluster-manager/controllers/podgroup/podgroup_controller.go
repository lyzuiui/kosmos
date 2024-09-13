package podgroup

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"

	"github.com/kosmos.io/kosmos/cmd/clustertree/cluster-manager/app/options"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"

	//clustertreeutils "github.com/kosmos.io/kosmos/pkg/clustertree/cluster-manager/utils"
	leafUtils "github.com/kosmos.io/kosmos/pkg/clustertree/cluster-manager/utils"
	"github.com/kosmos.io/kosmos/pkg/utils"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const (
	PodGroupControllerName = "podgroup-controller"
)

// PodGroupReconciler 监听主集群中的 PodGroup
type PodGroupReconciler struct {
	client.Client
	RootClient              client.Client
	SubClusterClients       []dynamic.Interface // 子集群的动态客户端列表 这可能还要自己找
	DynamicRootClient       dynamic.Interface
	envResourceManager      utils.EnvResourceManager
	GlobalLeafManager       leafUtils.LeafResourceManager
	GlobalLeafClientManager leafUtils.LeafClientResourceManager

	Options *options.Options
}

type envResourceManager struct {
	DynamicRootClient dynamic.Interface
}

func (r *PodGroupReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {

	// 使用 controller-runtime 获取主集群中的 PodGroup
	podGroup := &unstructured.Unstructured{}
	podGroup.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "scheduling.volcano.sh",
		Version: "v1beta1",
		Kind:    "PodGroup",
	})

	if err := r.Client.Get(ctx, req.NamespacedName, podGroup); err != nil {
		if errors.IsNotFound(err) {
			nodeNames := r.GlobalLeafManager.ListNodes()
			for _, nodeName := range nodeNames {
				lr, err := r.GlobalLeafManager.GetLeafResourceByNodeName(nodeName)
				if err != nil {
					// wait for leaf resource init
					return reconcile.Result{RequeueAfter: utils.DefaultRequeueTime}, nil
				}
				if err := r.syncDeleteAllPodGroupInLeafCluster(ctx, lr, req.NamespacedName); err != nil {
					klog.Errorf("delete podgroup in leaf error[2]: %v,  %s", err, req.NamespacedName)
					return reconcile.Result{RequeueAfter: utils.DefaultRequeueTime}, nil
				}
			}
			return reconcile.Result{}, nil
		}
		klog.Errorf("get %s error: %v", req.NamespacedName, err)
		return reconcile.Result{RequeueAfter: utils.DefaultRequeueTime}, nil
	}
	// 选择需要同步的子集群
	annotations := podGroup.GetAnnotations()
	selectedSubClusters, err := r.selectSubClusters(annotations)
	if annotations != nil {
		log.Printf("Failed to select sub clusters: %v", err)
	}
	log.Printf("Selected sub clusters: %v", selectedSubClusters)
	log.Printf("Annotations: %v", fmt.Sprintf("%v", annotations))

	if !podGroup.GetDeletionTimestamp().IsZero() {
		if err := r.syncDeleteToSubClusters(ctx, podGroup, selectedSubClusters); err != nil {
			klog.Errorf("delete podgroup in leaf error[1]: %v,  %s", err, req.NamespacedName)
			return reconcile.Result{RequeueAfter: utils.DefaultRequeueTime}, nil
		}
		return reconcile.Result{}, nil
	}

	// 同步 PodGroup 到选定的子集群
	log.Printf("Syncing PodGroup %s/%s to selected sub clusters", req.Namespace, req.Name)
	r.syncToSelectedSubClusters(ctx, podGroup, selectedSubClusters)

	return ctrl.Result{}, nil

}

// GetConfigMap retrieves the specified config map from the cache.
func (rm *envResourceManager) GetConfigMap(name, namespace string) (*corev1.ConfigMap, error) {
	// return rm.configMapLister.ConfigMaps(namespace).Get(name)
	obj, err := rm.DynamicRootClient.Resource(utils.GVR_CONFIGMAP).Namespace(namespace).Get(context.TODO(), name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}

	retObj := &corev1.ConfigMap{}
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(obj.UnstructuredContent(), &retObj); err != nil {
		return nil, err
	}

	return retObj, nil
}

func NewEnvResourceManager(client dynamic.Interface) utils.EnvResourceManager {
	return &envResourceManager{
		DynamicRootClient: client,
	}
}

func (r *PodGroupReconciler) SetupWithManager(mgr manager.Manager) error {

	if r.Client == nil {
		r.Client = mgr.GetClient()
	}

	r.envResourceManager = NewEnvResourceManager(r.DynamicRootClient)
	// 设置控制器管理器
	return ctrl.NewControllerManagedBy(mgr).
		For(&unstructured.Unstructured{
			Object: map[string]interface{}{
				"apiVersion": "scheduling.volcano.sh/v1beta1",
				"kind":       "PodGroup",
			},
		}).         // 监听 PodGroup 资源
		Complete(r) // 完成控制器的设置
}

func (rm *envResourceManager) GetSecret(name, namespace string) (*corev1.Secret, error) {
	// return rm.secretLister.Secrets(namespace).Get(name)
	obj, err := rm.DynamicRootClient.Resource(utils.GVR_SECRET).Namespace(namespace).Get(context.TODO(), name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}

	retObj := &corev1.Secret{}
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(obj.UnstructuredContent(), &retObj); err != nil {
		return nil, err
	}

	return retObj, nil
}

// ListServices retrieves the list of services from Kubernetes.
func (rm *envResourceManager) ListServices() ([]*corev1.Service, error) {
	// return rm.serviceLister.List(labels.Everything())
	objs, err := rm.DynamicRootClient.Resource(utils.GVR_SERVICE).List(context.TODO(), metav1.ListOptions{
		LabelSelector: labels.Everything().String(),
	})

	if err != nil {
		return nil, err
	}

	retObj := make([]*corev1.Service, 0)

	for _, obj := range objs.Items {
		tmpObj := &corev1.Service{}
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(obj.UnstructuredContent(), &tmpObj); err != nil {
			return nil, err
		}
		retObj = append(retObj, tmpObj)
	}

	return retObj, nil
}

func (r *PodGroupReconciler) leafClientResource(lr *leafUtils.LeafResource) (*leafUtils.LeafClientResource, error) {
	actualClusterName := leafUtils.GetActualClusterName(lr.Cluster)
	lcr, err := r.GlobalLeafClientManager.GetLeafResource(actualClusterName)
	if err != nil {
		return nil, fmt.Errorf("get leaf client resource err: %v", err)
	}
	return lcr, nil
}

// syncToSelectedSubClusters 只同步 PodGroup 到选定的子集群
func (r *PodGroupReconciler) syncToSelectedSubClusters(ctx context.Context, podGroup *unstructured.Unstructured, selectedSubClusters []string) error {

	// 从主集群的 PodGroup 中提取 metadata 和 spec 中的必要字段
	name := podGroup.GetName()
	namespace := podGroup.GetNamespace()
	spec, found, err := unstructured.NestedMap(podGroup.Object, "spec")
	if err != nil || !found {
		return fmt.Errorf("failed to retrieve spec from PodGroup: %v", err)
	}

	// 构建一个新的 PodGroup 对象，仅同步部分字段
	newPodGroup := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "scheduling.volcano.sh/v1beta1",
			"kind":       "PodGroup",
			"metadata": map[string]interface{}{
				"name":      name,
				"namespace": namespace,
			},
			"spec": map[string]interface{}{
				"minMember": spec["minMember"],
				"queue":     spec["queue"],
			},
		},
	}
	for _, clusterName := range selectedSubClusters {

		// 根据 clusterName 获取对应的动态客户端
		lr, err := r.GlobalLeafManager.GetLeafResourceByNodeName(clusterName)
		if err != nil {
			// 错误处理: 获取 LeafResource 失败
			return fmt.Errorf("Error: Unable to get LeafResource for cluster %s: %v", clusterName, err)
		}

		lcr, err := r.leafClientResource(lr)
		if err != nil {
			return fmt.Errorf("Failed to get leaf client resource for cluster %s: %v", clusterName, err)
		}
		_, err = lcr.DynamicClient.Resource(schema.GroupVersionResource{
			Group:    "scheduling.volcano.sh",
			Version:  "v1beta1",
			Resource: "podgroups",
		}).Namespace(podGroup.GetNamespace()).Get(ctx, name, metav1.GetOptions{})

		if err == nil {
			continue
		}

		if errors.IsNotFound(err) {
			unstructured.RemoveNestedField(podGroup.Object, "metadata", "resourceVersion")
			// 在子集群中创建 PodGroup
			_, err = lcr.DynamicClient.Resource(schema.GroupVersionResource{
				Group:    "scheduling.volcano.sh",
				Version:  "v1beta1",
				Resource: "podgroups",
			}).Namespace(podGroup.GetNamespace()).Create(ctx, newPodGroup, metav1.CreateOptions{})
			if err != nil {
				if errors.IsAlreadyExists(err) {
					continue
				}
				klog.Errorf("Failed to create PodGroup in cluster %s: %v", clusterName, err)
				continue
			}
			klog.V(4).Infof("Successfully created PodGroup %s/%s in cluster %s", podGroup.GetNamespace(), podGroup.GetName(), clusterName)
			continue
		} else {
			klog.V(4).Infof("Failed to check PodGroup in cluster %s: %v", clusterName, err)
			continue
		}
	}
	return nil
}

func (r *PodGroupReconciler) syncDeleteToSubClusters(ctx context.Context, podGroup *unstructured.Unstructured, selectedSubClusters []string) error {

	klog.V(4).Infof("Deleting podgroup %v/%+v", podGroup.GetNamespace(), podGroup.GetName())

	gvr := schema.GroupVersionResource{
		Group:    "scheduling.volcano.sh",
		Version:  "v1beta1",
		Resource: "podgroups",
	}

	for _, nodeName := range selectedSubClusters {
		lr, err := r.GlobalLeafManager.GetLeafResourceByNodeName(nodeName)
		if err != nil {
			klog.Errorf("Failed to get leaf client resource %v", lr.Cluster.Name)
			return fmt.Errorf("could not get leaf client resource: %v", err)
		}

		lcr, err := r.leafClientResource(lr)
		if err != nil {
			return fmt.Errorf("failed to get leaf client resource for cluster %s: %v", nodeName, err)
		}

		_, err = lcr.DynamicClient.Resource(gvr).Namespace(podGroup.GetNamespace()).Get(ctx, podGroup.GetName(), metav1.GetOptions{})
		if errors.IsNotFound(err) {
			klog.V(4).Infof("PodGroup %s/%s not found in sub cluster, skipping delete", podGroup.GetNamespace(), podGroup.GetName())
			continue
		} else if err != nil {
			klog.V(4).Infof("Failed to check PodGroup %s/%s in sub cluster: %v", podGroup.GetNamespace(), podGroup.GetName(), err)
			continue
		}

		err = lcr.DynamicClient.Resource(gvr).Namespace(podGroup.GetNamespace()).Delete(ctx, podGroup.GetName(), metav1.DeleteOptions{})
		if err != nil {
			klog.V(4).Infof("Failed to delete PodGroup %s/%s in sub cluster: %v", podGroup.GetNamespace(), podGroup.GetName(), err)
		} else {
			klog.V(4).Infof("Successfully deleted PodGroup %s/%s in sub cluster", podGroup.GetNamespace(), podGroup.GetName())
		}
	}
	return nil
}

func (r *PodGroupReconciler) syncDeleteAllPodGroupInLeafCluster(ctx context.Context, lr *leafUtils.LeafResource, rootnamespacedname types.NamespacedName) error {

	klog.V(4).Infof("Deleting podgroup %v/%+v", rootnamespacedname.Namespace, rootnamespacedname.Name)

	leafPodGroup := &unstructured.Unstructured{}
	leafPodGroup.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "scheduling.volcano.sh",
		Version: "v1beta1",
		Kind:    "PodGroup",
	})

	gvr := schema.GroupVersionResource{
		Group:    "scheduling.volcano.sh",
		Version:  "v1beta1",
		Resource: "podgroups",
	}

	lcr, err := r.leafClientResource(lr)
	if err != nil {
		klog.Errorf("Failed to get leaf client resource %v", lr.Cluster.Name)
		return fmt.Errorf("could not get leaf client resource: %v", err)
	}
	_, err = lcr.DynamicClient.Resource(gvr).Namespace(rootnamespacedname.Namespace).Get(ctx, rootnamespacedname.Name, metav1.GetOptions{})
	if errors.IsNotFound(err) {
		klog.Errorf("PodGroup %s/%s not found in sub cluster, skipping delete", rootnamespacedname.Namespace, rootnamespacedname.Name)
		return err
	} else if err != nil {
		klog.Errorf("Failed to check PodGroup %s/%s in sub cluster: %v", rootnamespacedname.Namespace, rootnamespacedname.Name, err)
		return err
	}

	err = lcr.DynamicClient.Resource(gvr).Namespace(rootnamespacedname.Namespace).Delete(ctx, rootnamespacedname.Name, metav1.DeleteOptions{})
	if err != nil {
		klog.Errorf("Failed to delete PodGroup %s/%s in sub cluster: %v", rootnamespacedname.Namespace, rootnamespacedname.Name, err)
		return err
	} else {
		klog.V(4).Infof("Successfully deleted PodGroup %s/%s in sub cluster", rootnamespacedname.Namespace, rootnamespacedname.Name)
	}
	return nil
}

func (r *PodGroupReconciler) selectSubClusters(annotations map[string]string) ([]string, error) {
	lastAppliedConfig, ok := annotations["kubectl.kubernetes.io/last-applied-configuration"]
	if !ok {
		return nil, fmt.Errorf("no last-applied-configuration found in annotations")
	}

	var config map[string]interface{}
	if err := json.Unmarshal([]byte(lastAppliedConfig), &config); err != nil {
		return nil, fmt.Errorf("error parsing JSON: %v", err)
	}
	spec, ok := config["spec"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("no spec found in configuration")
	}

	tasks, ok := spec["tasks"].([]interface{})
	if !ok {
		return nil, fmt.Errorf("no tasks found in spec")
	}

	var nodeNames []string
	for _, task := range tasks {
		taskMap := task.(map[string]interface{})
		if taskMap["name"] == "worker" {
			template := taskMap["template"].(map[string]interface{})
			spec := template["spec"].(map[string]interface{})
			affinity, ok := spec["affinity"].(map[string]interface{})
			if !ok {
				continue
			}
			nodeAffinity, ok := affinity["nodeAffinity"].(map[string]interface{})
			if !ok {
				continue
			}
			required := nodeAffinity["requiredDuringSchedulingIgnoredDuringExecution"].(map[string]interface{})
			nodeSelectorTerms := required["nodeSelectorTerms"].([]interface{})
			for _, term := range nodeSelectorTerms {
				termMap := term.(map[string]interface{})
				matchExpressions := termMap["matchExpressions"].([]interface{})
				for _, expr := range matchExpressions {
					exprMap := expr.(map[string]interface{})
					if exprMap["key"] == "kubernetes.io/hostname" {
						values := exprMap["values"].([]interface{})
						for _, value := range values {
							nodeName := strings.TrimPrefix(value.(string), "kosmos-")
							nodeNames = append(nodeNames, nodeName)
						}
					}
				}
			}
		}
	}

	if len(nodeNames) == 0 {
		return nil, fmt.Errorf("no nodes found in node affinity")
	}

	return nodeNames, nil
}
