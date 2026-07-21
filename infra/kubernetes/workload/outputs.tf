output "namespace" { value = kubernetes_namespace_v1.sandboxes.metadata[0].name }
output "service_name" { value = kubernetes_service_v1.control.metadata[0].name }
output "runtime_class_name" { value = kubernetes_runtime_class_v1.runc.metadata[0].name }
