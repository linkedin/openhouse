resource "helm_release" "tables" {
  chart = "tables-service"
  name = "tables-service"
  repository = "" // path of chart
  namespace = "application"
}