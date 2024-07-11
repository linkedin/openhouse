resource "kubernetes_namespace" "sandbox" {
  metadata {
    name = "sandbox"
  }
}
