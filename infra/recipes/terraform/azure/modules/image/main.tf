resource "docker_image" "housetables" {
    name = "${var.registry_login_server}/openhouse-housetables-service:latest"

    build {
        context = "${path.cwd}/../../../../../.."
        dockerfile = "housetables-service.Dockerfile"
    }
}

resource "docker_registry_image" "registry" {
    name = "${docker_image.housetables.name}"

    depends_on = [ docker_image.housetables ]
}
