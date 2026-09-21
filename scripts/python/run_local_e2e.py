"""Build a disposable Docker deployment and run both HTTP E2E suites."""

import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
from urllib.error import HTTPError, URLError
from urllib.request import urlopen
import uuid
import venv

ROOT = Path(__file__).resolve().parents[2]
RECIPE = ROOT / 'infra/recipes/docker-compose/oh-only-mysql/docker-compose.yml'


def run(*command, **kwargs):
    return subprocess.run(command, cwd=ROOT, check=True, **kwargs)


def wait_ready(url, expected):
    deadline = time.monotonic() + 180
    last_status = 'not reachable'
    while time.monotonic() < deadline:
        try:
            with urlopen(url, timeout=3) as response:
                last_status = response.status
        except HTTPError as error:
            last_status = error.code
            error.close()
        except (URLError, TimeoutError, ConnectionError) as error:
            last_status = str(error)
        if last_status in expected:
            return
        time.sleep(2)
    raise RuntimeError(f'Timed out waiting for {url}: {last_status}')


def interrupt(signum, frame):
    raise KeyboardInterrupt


def main():
    if len(sys.argv) != 1:
        print('Usage: python3 scripts/python/run_local_e2e.py')
        return 0 if sys.argv[1:] == ['--help'] else 1

    run('docker', 'info', '--format', '{{.ServerVersion}}')
    project = f'openhouse-e2e-{uuid.uuid4().hex[:10]}'
    print(f'Using disposable Compose project {project}', flush=True)
    with tempfile.TemporaryDirectory(prefix='openhouse-e2e-') as temporary:
        temporary = Path(temporary)
        override = temporary / 'compose.yml'
        # Random loopback ports and a private OPA name leave other stacks alone.
        override.write_text(f'''services:
  openhouse-tables:
    ports: !override ["127.0.0.1::8080"]
  openhouse-housetables:
    ports: !override ["127.0.0.1::8080"]
  mysql:
    ports: !override ["127.0.0.1::3306"]
  prometheus:
    ports: !override []
  opa:
    container_name: {project}-opa
    ports: !override []
''')
        compose = ['docker', 'compose', '-p', project, '-f', str(RECIPE),
                   '-f', str(override)]
        run(*compose, 'config', '--quiet')
        run('./gradlew', ':services:housetables:bootJar', ':services:tables:bootJar',
            '-x', 'CopyGitHooksTask')
        venv.create(temporary / 'venv', with_pip=True)
        python = str(temporary / 'venv/bin/python')
        run(python, '-m', 'pip', 'install', '--quiet',
            '-r', 'scripts/python/requirements.txt')

        try:
            run(*compose, 'up', '-d', '--build', 'openhouse-tables')

            def address(service, port):
                return run(*compose, 'port', service, str(port),
                           capture_output=True, text=True).stdout.strip()

            tables_host = f'http://{address("openhouse-tables", 8080)}'
            hts_host = f'http://{address("openhouse-housetables", 8080)}'
            db_port = address('mysql', 3306).rsplit(':', 1)[1]
            print(f'Waiting for Tables at {tables_host} and HTS at {hts_host}', flush=True)
            wait_ready(f'{hts_host}/hts/tables/query', {200})
            wait_ready(f'{tables_host}/v1/databases', {200, 401})
            env = dict(os.environ, OPENHOUSE_TABLES_HOST=tables_host,
                       HTS_DB_HOST='127.0.0.1', HTS_DB_PORT=db_port,
                       HTS_DB_USER='oh_user', HTS_DB_PASSWORD='oh_password',
                       HTS_DB_NAME='oh_db')
            run(python, 'scripts/python/hts_integration_test.py', hts_host,
                '--require-database', env=env)
            run(python, 'scripts/python/integration_test.py',
                'tables-test-fixtures/tables-test-fixtures-iceberg-1.2/'
                'src/main/resources/dummy.token', env=env)
        except (subprocess.CalledProcessError, RuntimeError, OSError, KeyboardInterrupt):
            subprocess.run([*compose, 'logs', '--tail=100'], cwd=ROOT, check=False)
            raise
        finally:
            print(f'Removing disposable deployment {project}', flush=True)
            run(*compose, 'down', '--volumes', '--remove-orphans')
    print('Both E2E suites passed; test containers and database removed.')
    return 0


if __name__ == '__main__':
    signal.signal(signal.SIGTERM, interrupt)
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print('E2E run interrupted.', file=sys.stderr)
        sys.exit(130)
    except (subprocess.CalledProcessError, RuntimeError, OSError) as error:
        print(f'E2E run failed: {error}', file=sys.stderr)
        sys.exit(1)
