#!/usr/bin/env python3
from builtins import str, staticmethod, input, range, repr, map, int, type

import argparse
import json
import pathlib

import requests
import time
from contextlib import contextmanager
from pprint import pprint
from requests import Response
from typing import Tuple, Optional, Any, Dict


LIVY_ENDPOINT = 'http://localhost:9003'
TABLES_ENDPOINT = 'http://localhost:8000'

LIVY_SESSIONS_ENDPOINT = f'{LIVY_ENDPOINT}/sessions'
HEADERS = {'Content-Type': 'application/json'}
TABLES_SERVICE_TOKEN_FILE = pathlib.Path('services/common/src/main/resources/dummy.token')
TABLES_SERVICE_TOKEN = TABLES_SERVICE_TOKEN_FILE.read_text()
SESSION_REQUEST_DATA_TEMPLATE = {
	'conf': {
		'spark.jars.packages': 'org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.5.0',
		'spark.sql.extensions': 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,com.linkedin.openhouse.spark.extensions.OpenhouseSparkSessionExtensions',
		'spark.sql.catalog.openhouse': 'org.apache.iceberg.spark.SparkCatalog',
		'spark.sql.catalog.openhouse.catalog-impl': 'com.linkedin.openhouse.spark.OpenHouseCatalog',
		'spark.sql.catalog.openhouse.uri': 'http://openhouse-tables:8080',
		'spark.sql.catalog.openhouse.auth-token': TABLES_SERVICE_TOKEN,
		'spark.sql.catalog.openhouse.cluster': 'LocalHadoopCluster'
	}
}


class TablesClient:
	TABLES_HEADERS = {
		'Content-Type': 'application/json',
		'Authorization': f'Bearer {TABLES_SERVICE_TOKEN}'
	}

	def __init__(self, base_url: str):
		self.base_url = base_url

	def drop_table_if_exists(self, db: str, table: str):
		url = f'{self.base_url}/databases/{db}/tables/{table}'
		response = requests.delete(url, headers=self.TABLES_HEADERS)
		pprint(response.status_code)
		pprint(response.json())
		assert response.status_code in {requests.codes.NO_CONTENT, requests.codes.NOT_FOUND}

	def get_table_json(self, db: str, table: str) -> 'json':
		url = f'{self.base_url}/databases/{db}/tables/{table}'
		response = requests.get(url, headers=self.TABLES_HEADERS)
		assert response.status_code in {requests.codes.OK}
		return response.json()

	def set_table_retention(self, db: str, table: str, ttl_days: int) -> None:
		table_json = self.get_table_json(db, table)
		table_json['policies'] = {
			'retention': {
				'days': ttl_days
			}
		}
		url = f'{self.base_url}/databases/{db}/tables/{table}'
		response = requests.put(url, headers=self.TABLES_HEADERS, data=json.dumps(table_json))
		assert response.status_code in {requests.codes.CREATED, requests.codes.OK}


class LivySession:
	def __init__(self, id: str, url: str):
		self.id = id
		self.url = url

	@staticmethod
	def create_default(kind: Optional[str] = 'spark') -> 'LivySession':
		return LivySession(*create_session(kind=kind))

	def close(self) -> None:
		delete_session(self.url)

	def run_statement(self, cmd: str) -> None:
		run_statement(self.url + '/statements', cmd)


def print_response(response: Response) -> None:
	pprint('Response json:')
	pprint(response.json(), indent=2)


def get_session_state(session_url: str) -> Response:
	ret = requests.get(session_url, headers=HEADERS)
	print_response(ret)
	assert ret.status_code == requests.codes.OK, 'could not fetch session state'
	return ret


def get_statement_state(statement_url: str) -> Response:
	ret = requests.get(statement_url, headers=HEADERS)
	print_response(ret)
	assert ret.status_code == requests.codes.OK, 'could not fetch statement state'
	return ret


def wait_for_session_ready(session_url: str) -> None:
	response = get_session_state(session_url)
	while response.json()['state'] != 'idle':
		pprint(f'Waiting for the session to start, current state: {response.json()["state"]}')
		time.sleep(1)
		response = get_session_state(session_url)


def delete_session(session_url: str) -> None:
	# note that in Spark standalone mode an app takes all resources by default, other apps will wait
	pprint('Deleting session')
	response = requests.delete(session_url, headers=HEADERS)
	print_response(response)
	assert response.status_code == requests.codes.OK, 'could not delete session'


def wait_for_statement_complete(statement_url: str) -> None:
	response = get_statement_state(statement_url)
	while response.json()['state'] not in ['error', 'cancelled', 'available']:
		pprint(f'Waiting for the statement to complete, current state: {response.json()["state"]}')
		time.sleep(1)
		response = get_statement_state(statement_url)


def run_statement(statements_url: str, cmd: str) -> None:
	data = {
		'code': cmd
	}
	pprint(f'Running statement: {cmd}')
	response = requests.post(statements_url, data=json.dumps(data), headers=HEADERS)
	print_response(response)
	assert response.status_code == requests.codes.CREATED, 'statement creation failed'

	statement_url = LIVY_ENDPOINT + response.headers['location']
	wait_for_statement_complete(statement_url)
	response = get_statement_state(statement_url)
	pprint('#'*10 + " output " + '#'*10)
	output = response.json()['output']
	if 'ename' in output:
		pprint(output['ename'])
		pprint(output['evalue'])
	else:
		pprint(output['data']['application/json']['data'])
	pprint('#'*28)


def add_spark_jars(data: Dict[str, Any]) -> None:
	data['conf']['spark.jars'] = 'local:/opt/spark/openhouse-spark-runtime_2.12-latest-all.jar'


def create_session(kind: Optional[str] = 'spark') -> Tuple[str, str]:
	data = SESSION_REQUEST_DATA_TEMPLATE.copy()
	data['kind'] = kind
	add_spark_jars(data)
	pprint(f'Creating Spark session with config: {data}')
	response = requests.post(LIVY_SESSIONS_ENDPOINT, data=json.dumps(data), headers=HEADERS)
	print_response(response)
	assert response.status_code == requests.codes.CREATED, 'session creation failed'

	# wait for the session initialization
	session_url = LIVY_ENDPOINT + response.headers['location']
	wait_for_session_ready(session_url)

	return response.json()['id'], session_url


@contextmanager
def managed_session(*args, **kwargs):
	session = LivySession.create_default(*args, **kwargs)
	try:
		yield session
	finally:
		session.close()


def run_server_test() -> None:
	with managed_session(kind='spark') as session:
		session: LivySession
		# run simple session statement, just check it server accepts
		session.run_statement('0 / 0')
	pprint('Livy server test succeeded, please check logs for details')


def run_table_test() -> None:
	with managed_session(kind='sql') as session:
		session: LivySession
		session.run_statement('DROP TABLE IF EXISTS openhouse.db.tb')
		session.run_statement('CREATE TABLE openhouse.db.tb (ts timestamp, col1 string, col2 string) PARTITIONED BY (days(ts))')
		session.run_statement('DESCRIBE TABLE openhouse.db.tb')
		session.run_statement('SELECT * FROM openhouse.db.tb')
		session.run_statement('SHOW TABLES IN openhouse.db')
		input(f'''
			Test completed.
			Check Livy UI {LIVY_ENDPOINT}/ui/session/{session.id} to inspect statements of current session.
			Press <enter> to finish and destroy the session.
		''')
	pprint('Table test succeeded, please check logs for details')


def run_sql_repl() -> None:
	with managed_session(kind='sql') as session:
		session: LivySession
		pprint(f'Check Livy UI {LIVY_ENDPOINT}/ui/session/{session.id} to inspect statements of current session')
		while True:
			sql = input("Input SQL statement> ")
			session.run_statement(sql)


def run_batches_sparkpi_test() -> None:
	data = {
		'className': 'org.apache.spark.examples.SparkPi',
		'file': 'local:/opt/spark/examples/jars/spark-examples_2.12-3.0.2.jar',
		'args': ['100']
	}
	livy_batches_endpoint = LIVY_ENDPOINT + '/batches'
	response = requests.post(livy_batches_endpoint, data=json.dumps(data), headers=HEADERS)
	assert response.status_code == requests.codes.CREATED, 'batch POST failed'
	pprint(response.json())
	this_batch_endpoint = livy_batches_endpoint + '/' + str(response.json()['id'])
	response = requests.get(this_batch_endpoint, headers=HEADERS)
	pprint(response.json())
	while response.json()['state'] not in ['success', 'failure']:
		response = requests.get(this_batch_endpoint, headers=HEADERS)
		pprint(response.json())
		time.sleep(1)
	assert response.json()['state'] == 'success', 'test job failed'
	pprint('Batch test succeeded')


def recreate_time_partitioned_table(session: LivySession, client: TablesClient, db_name: str, table_name: str, ttl_days: Optional[int] = None) -> None:
	shift_days = (ttl_days or 1) + 1
	num_rows = 10
	fqtn = f'{db_name}.{table_name}'
	client.drop_table_if_exists(db_name, table_name)
	session.run_statement(f'CREATE TABLE {fqtn} (id int, ts timestamp, col string) PARTITIONED BY (days(ts), col)')
	session.run_statement(f'DESCRIBE TABLE {fqtn}')
	values = [
		f'({i}, current_timestamp(), {i})' for i in range(num_rows)
	]
	insert_sql = f'''
		INSERT INTO {fqtn} VALUES {','.join(values)}
	'''
	session.run_statement(insert_sql)
	insert_shifted_sql = f'''
		INSERT INTO {fqtn} SELECT id, ts - INTERVAL {shift_days} days, col FROM {fqtn}
	'''
	session.run_statement(insert_shifted_sql)
	if ttl_days:
		client.set_table_retention(db=db_name, table=table_name, ttl_days=ttl_days)


def prepare_tables_for_jobs() -> None:
	tables_client = TablesClient(base_url=TABLES_ENDPOINT)

	with managed_session(kind='sql') as session:
		session: LivySession
		session.run_statement('USE openhouse')
		recreate_time_partitioned_table(session, tables_client, 'db', 'no_retention')
		recreate_time_partitioned_table(session, tables_client, 'db', 'retention', ttl_days=7)
		input(f'''
			Tables preparation completed.
			Check Livy UI {LIVY_ENDPOINT}/ui/session/{session.id} to inspect statements of current session.
			Press <enter> to finish and destroy the session.
		''')
	pprint('Tables for jobs test prepared')


REGISTRY = {
	'livy_server': run_server_test,
	'table_sql': run_table_test,
	'sql_repl': run_sql_repl,
	'pi_test_batches': run_batches_sparkpi_test,
	'prepare_tables_for_jobs': prepare_tables_for_jobs,
}


def main(cmd_args) -> None:
	REGISTRY[cmd_args.type]()


if __name__ == "__main__":
	parser = argparse.ArgumentParser(description='Livy endpoint integration test')
	parser.add_argument('-t', dest='type', type=str, choices=REGISTRY.keys(), help='types of test')
	main(parser.parse_args())
