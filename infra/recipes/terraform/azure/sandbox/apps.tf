resource "kubernetes_namespace" "sandbox" {
  metadata {
    name = "sandbox"
  }
}

# use a format like this: 
# resource "kubernetes_deployment" "..." {
# }


# resource "kubernetes_service" "..." {
# }