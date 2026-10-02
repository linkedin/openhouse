import unittest
from unittest.mock import Mock, patch

import table_hts_integration_test as integration


def response(body=None, status=200):
    result = Mock(status_code=status, text=str(body))
    result.json.return_value = body
    return result


class TableHtsHelpersTest(unittest.TestCase):
    def test_payload_uses_current_metadata_pointer_as_version(self):
        payload = integration.table_payload('db', 'table', '/tmp/current.json', {})
        self.assertEqual(payload['baseTableVersion'], '/tmp/current.json')
        self.assertEqual(payload['tableProperties'], {})
        self.assertEqual(integration.table_payload('db', 'table')['baseTableVersion'],
                         integration.INITIAL_VERSION)

    @patch.object(integration, 'read_hts')
    def test_table_pointer_agrees_across_typed_and_neutral_reads(self, read):
        entity = {'entityType': 'TABLE', 'metadataLocation': '/tmp/metadata.json'}
        read.side_effect = [response({'entity': entity}), response({'entity': entity}),
                            response(status=404)]
        self.assertEqual(integration.assert_table_pointer(
            'db', 'table', {'tableLocation': 'file:/tmp/metadata.json'}), entity)

    @patch.object(integration, 'read_hts')
    def test_pointer_disagreement_is_not_a_success(self, read):
        for typed in (
            {'entityType': 'VIEW', 'metadataLocation': '/tmp/metadata.json'},
            {'entityType': 'TABLE', 'metadataLocation': '/tmp/other.json'},
        ):
            with self.subTest(typed=typed):
                read.side_effect = [
                    response({'entity': {
                        'entityType': 'TABLE', 'metadataLocation': '/tmp/metadata.json'}}),
                    response({'entity': typed}),
                ]
                with self.assertRaises(AssertionError):
                    integration.assert_table_pointer(
                        'db', 'table', {'tableLocation': '/tmp/metadata.json'})

    @patch.object(integration.requests, 'delete')
    def test_cleanup_tolerates_absence_but_not_server_errors(self, delete):
        for status in (204, 404):
            delete.return_value = response(status=status)
            integration.cleanup_table('http://localhost/table', {})
        delete.assert_called_with('http://localhost/table', headers={},
                                  params={'purge': True}, timeout=integration.TIMEOUT)
        delete.return_value = response(status=500)
        with self.assertRaises(AssertionError):
            integration.cleanup_table('http://localhost/table', {})


if __name__ == '__main__':
    unittest.main()
