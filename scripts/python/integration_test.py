import requests
import sys

DATABASE_ID = "d3"
TABLE_ID = "t1"


def test_create_table(token_str: str) -> None:
    create_table_url = f'http://localhost:8000/v1/databases/{DATABASE_ID}/tables/'
    create_table_payload = {
        "tableId": TABLE_ID,
        "databaseId": DATABASE_ID,
        "baseTableVersion": "INITIAL_VERSION",
        "clusterId": "LocalFSCluster",
        "schema": "{\"type\": \"struct\", \"fields\": [{\"id\": 1,\"required\": true,\"name\": \"id\",\"type\": \"string\"},{\"id\": 2,\"required\": true,\"name\": \"name\",\"type\": \"string\"},{\"id\": 3,\"required\": true,\"name\": \"ts\",\"type\": \"timestamp\"}]}",
        "timePartitioning": {
            "columnName": "ts",
            "granularity": "HOUR"
        },
        "clustering": [
            {
                "columnName": "name"
            }
        ],
        "tableProperties": {
            "key": "value"
        }
    }
    headers = {'Content-Type': 'application/json', "Authorization": f"Bearer {token_str}"}

    response = requests.post(create_table_url, json=create_table_payload, headers=headers)
    assert response.status_code == 201, f"Failed to create table: {response.status_code} {response.text}"
    print("Table created successfully with response: ", response.json())

def test_get_table(token_str: str) -> None:
    get_table_url = f'http://localhost:8000/v1/databases/{DATABASE_ID}/tables/{TABLE_ID}'
    headers = {'Content-Type': 'application/json', 'Authorization': f'Bearer {token_str}'}
    response = requests.get(get_table_url, headers=headers)
    assert response.status_code == 200, f"Failed to get table: {response.status_code} {response.text}"
    print("Table retrieved successfully with response: ", response.json())

def test_get_table_not_found(token_str: str) -> None:
    get_table_url = f'http://localhost:8000/v1/databases/{DATABASE_ID}/tables/{TABLE_ID}'
    headers = {'Content-Type': 'application/json', 'Authorization': f'Bearer {token_str}'}
    response = requests.get(get_table_url, headers=headers)
    assert response.status_code == 404, f"Table found when expected to be not found: {response.status_code} {response.text}"
    print("Table not found as expected with response: ", response.status_code)

def test_delete_table(token_str: str) -> None:
    delete_table_url = f'http://localhost:8000/v1/databases/{DATABASE_ID}/tables/{TABLE_ID}'
    headers = {'Content-Type': 'application/json', 'Authorization': f'Bearer {token_str}'}
    response = requests.delete(delete_table_url, headers=headers)
    assert response.status_code == 204, f"Failed to delete table: {response.status_code} {response.text}"
    print("Table deleted successfully with response: ", response.status_code)

def read_file(file_name: str) -> str:
    try:
        with open(file_name, 'r') as file:
            file_contents = file.read()
    except FileNotFoundError:
        print(f"File {file_name} not found")
        sys.exit(1)
    return file_contents


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: python docker_integration_test.py <token_file>")
        sys.exit(1)

    token_file = sys.argv[1]
    token_str = read_file(token_file)

    test_get_table_not_found(token_str)
    test_create_table(token_str)
    test_get_table(token_str)
    test_delete_table(token_str)
    test_get_table_not_found(token_str)
    
    print("All tests passed successfully")

