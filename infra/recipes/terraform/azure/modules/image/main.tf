locals {
  codebase_root = "${abspath(path.module)}/../../../../../.."
}

resource "docker_image" "housetables" {
  name = "${var.registry_login_server}/openhouse-housetables-service:latest"

  build {
    context    = local.codebase_root
    dockerfile = "housetables-service.Dockerfile"
  }
}

resource "docker_registry_image" "hts_registry" {
  name = docker_image.housetables.name

  depends_on = [docker_image.housetables]
}

resource "docker_image" "tables" {
  name = "${var.registry_login_server}/openhouse-tables-service:latest"

  build {
    context    = local.codebase_root
    dockerfile = "tables-service.Dockerfile"
  }
}

resource "docker_registry_image" "tables_registry" {
  name = docker_image.tables.name

  depends_on = [docker_image.tables]
}
