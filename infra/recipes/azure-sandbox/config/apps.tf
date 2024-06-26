resource "kubernetes_namespace" "sandbox" {
  metadata {
    name = "sandbox"
  }
}

resource "kubernetes_deployment" "nginx_deployment" {
  metadata {
    name      = "sandbox"
    namespace = kubernetes_namespace.sandbox.metadata.0.name
  }
  spec {
    replicas = 2
    selector {
      match_labels = {
        app = "sandbox"
      }
    }
    template {
      metadata {
        labels = {
          app = "sandbox"
        }
      }
      spec {
        container {
          image = "nginx:1.19.4"
          name  = "nginx"

          resources {
            limits = {
              memory = "512M"
              cpu    = "1"
            }
            requests = {
              memory = "256M"
              cpu    = "50m"
            }
          }
        }
      }
    }
  }
}


resource "kubernetes_service" "nginx_service" {
  metadata {
    name      = "test"
    namespace = kubernetes_namespace.sandbox.metadata.0.name
  }
  spec {
    selector = {
      app = kubernetes_deployment.nginx_deployment.spec.0.template.0.metadata.0.labels.app
    }
    port {
      port        = 80
      target_port = 80
    }
    type = "LoadBalancer"
  }
}