import unittest
from unittest.mock import Mock, call, patch

import hts_integration_test as hts


def response(body, status=200):
    result = Mock(status_code=status, text=str(body), url='http://localhost/test')
    result.request.method = 'GET'
    result.json.return_value = body
    return result


def page(number, content, last):
    return response({
        'results': None,
        'pageResults': {'number': number, 'content': content, 'last': last},
    })


class QueryHelpersTest(unittest.TestCase):
    @patch.object(hts.requests, 'get')
    def test_views_use_versioned_route_and_collect_every_page(self, get):
        get.side_effect = [page(0, [{'tableId': 'v1'}], False),
                           page(1, [{'tableId': 'v2'}], True)]
        self.assertEqual(hts.query_entities('views', 'db'),
                         [{'tableId': 'v1'}, {'tableId': 'v2'}])
        self.assertEqual(get.call_args_list, [
            call(f'{hts.HOST}/v1/hts/views/query',
                 params={'databaseId': 'db', 'page': number, 'size': 50, 'sortBy': 'tableId'})
            for number in (0, 1)
        ])

    @patch.object(hts.requests, 'get')
    def test_tables_keep_legacy_envelope(self, get):
        get.return_value = response({'results': [{'tableId': 't'}], 'pageResults': None})
        self.assertEqual(hts.query_entities('tables', 'db'), [{'tableId': 't'}])
        get.assert_called_once_with(f'{hts.HOST}/hts/tables/query', params={'databaseId': 'db'})

    @patch.object(hts.requests, 'get')
    def test_empty_view_listing_is_valid(self, get):
        get.return_value = page(0, [], True)
        self.assertEqual(hts.query_entities('views', 'db'), [])

    @patch.object(hts.requests, 'get')
    def test_failure_on_later_page_is_not_partial_success(self, get):
        get.side_effect = [page(0, [{'tableId': 'v'}], False), response({}, 500)]
        with self.assertRaises(AssertionError):
            hts.query_entities('views', 'db')

    @patch.object(hts.requests, 'get')
    def test_repeated_or_empty_nonfinal_page_fails(self, get):
        for bad in (page(0, [], False), page(1, [], True)):
            with self.subTest(body=bad.json()):
                get.return_value = bad
                with self.assertRaises(AssertionError):
                    hts.query_entities('views', 'db')

    @patch.object(hts, 'query_soft_deleted', return_value=[])
    @patch.object(hts.requests, 'delete')
    @patch.object(hts.requests, 'get')
    def test_cleanup_fetches_all_pages_before_deleting(self, get, delete, soft_deleted):
        events = []

        def fetch(url, params):
            events.append('fetch')
            if url.endswith('/hts/tables/query'):
                return response({'results': [], 'pageResults': None})
            number = params['page']
            return page(number, [{'tableId': f'v{number}'}], number == 1)

        get.side_effect = fetch
        delete.side_effect = lambda *args, **kwargs: events.append('delete')
        hts.cleanup('db')
        self.assertEqual(events[:4], ['fetch', 'fetch', 'delete', 'delete'])
        self.assertEqual(delete.call_count, 2)


if __name__ == '__main__':
    unittest.main()
