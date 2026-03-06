import datetime
from datetime import date, timedelta
from unittest import TestCase, main
from moex.connector import MoexConnector, ConnectorModes

mc = MoexConnector(output_mode=ConnectorModes.DATAFRAME)


class TestEndpoints(TestCase):
    SECURITIES_COLUMNS = [
        'SECID', 'SHORTNAME', 'REGNUMBER', 'NAME', 'ISIN', 'IS_TRADED', 'EMITENT_ID',
        'EMITENT_TITLE', 'EMITENT_INN', 'EMITENT_OKPO', 'TYPE', 'GROUP',
        'PRIMARY_BOARDID', 'MARKETPRICE_BOARDID'
    ]

    SECURITY_COLUMNS = [
        'SECID', 'ISSUENAME', 'NAME', 'SHORTNAME', 'ISIN', 'REGNUMBER', 'ISSUESIZE',
        'FACEVALUE', 'FACEUNIT', 'ISSUEDATE', 'LATNAME', 'HASPROSPECTUS', 'DECISIONDATE',
        'HASDEFAULT', 'HASTECHNICALDEFAULT', 'EMITENTMISMATCHCUR', 'LISTLEVEL',
        'ISQUALIFIEDINVESTORS', 'MORNINGSESSION', 'EVENINGSESSION', 'WEEKENDSESSION',
        'REGISTRY_DATE', 'TYPENAME', 'GROUP', 'TYPE', 'GROUPNAME', 'EMITTER_ID'
    ]

    NEWS_COLUMNS = ['ID', 'TAG', 'TITLE', 'PUBLISHED_AT', 'MODIFIED_AT']

    EVENTS_COLUMNS = ['ID', 'TAG', 'TITLE', 'FROM', 'MODIFIED_AT']

    ENGINES_COLUMNS = ['ID', 'NAME', 'TITLE']
    MARKET_COLUMNS = ENGINES_COLUMNS
    BOARDS_COLUMNS = ['ID', 'BOARD_GROUP_ID', 'BOARDID', 'TITLE', 'IS_TRADED']
    CANDLES_COLUMNS = ['OPEN', 'CLOSE', 'HIGH', 'LOW', 'VALUE', 'VOLUME', 'BEGIN', 'END']

    def test_security(self):
        sc = mc.security('SBER')
        assert len(self.SECURITY_COLUMNS) == len(sc.columns.tolist())

    def test_site_news(self):
        nws = mc.sitenews()
        assert 50 == nws.shape[0]
        assert set(self.NEWS_COLUMNS) == set(nws.columns.str.upper().tolist())

    def test_events(self):
        evs = mc.events()
        assert set(self.EVENTS_COLUMNS) == set(evs.columns.str.upper().tolist())

    def test_securities(self):
        with self.subTest('Testing default behaviour', params=None):
            scs = mc.securities()
            assert 100 == scs.shape[0]
            assert set(self.SECURITIES_COLUMNS) == set(scs.columns.str.upper().tolist())
            assert len(scs.secid.unique().tolist()) == scs.shape[0]

        with self.subTest('Testing limit option', limit=5):
            scs = mc.securities(limit=5)
            assert 5 == scs.shape[0]

        with self.subTest('Testing start and limit options', start=7, limit=10):
            # 4 overlapping securities
            scs_0 = mc.securities(start=0, limit=10)
            ids_0 = scs_0.secid.unique().tolist()
            scs_1 = mc.securities(start=7, limit=10)
            ids_1 = scs_1.secid.unique().tolist()
            assert len(ids_0) == len(ids_1) == 10
            assert len((set(ids_0) - set(ids_1))) == 7
            assert len((set(ids_1) - set(ids_0))) == 7

        with self.subTest('Testing start option', start=10):
            # 0 overlapping securities
            scs_0 = mc.securities(start=0, limit=10)
            ids_0 = scs_0.secid.unique().tolist()
            scs_1 = mc.securities(start=10, limit=10)
            ids_1 = scs_1.secid.unique().tolist()
            assert len(ids_0) == len(ids_1) == 10
            assert len((set(ids_0) - set(ids_1))) == 10
            assert len((set(ids_1) - set(ids_0))) == 10

    def test_engines(self):
        e = mc.engines()
        assert (
            len([col for col in self.ENGINES_COLUMNS if col in e.columns.tolist()]) ==
            len(self.ENGINES_COLUMNS)
        )

    def test_markets(self):
        m = mc.markets('stock')
        assert (
                len([col for col in self.MARKET_COLUMNS if col in m.columns.tolist()]) ==
                len(self.MARKET_COLUMNS)
        )
        assert 0 == sum([x for x in m.isna().sum()])

    def test_boards(self):
        b = mc.boards('stock', 'shares')
        assert (
                len([col for col in self.BOARDS_COLUMNS if col in b.columns.tolist()]) ==
                len(self.BOARDS_COLUMNS)
        )

    def test_candles(self):
        from_date = datetime.date.today() - datetime.timedelta(days=5)
        c = mc.candles('stock', 'shares', 'SBER')
        assert (
                len([col for col in self.CANDLES_COLUMNS if col in c.columns.tolist()]) ==
                len(self.CANDLES_COLUMNS)
        )
        assert from_date == c['begin'].min().date()

    def test_other_endpoint(self):
        ts = mc.other_endpoint('turnovers', lang='ru')
        assert ts.shape[0] > 0

    def test_method_params(self):
        c = mc.candles('stock', 'shares', 'SBER')
        assert len(c.index) > 1
        # this test should fail if running on the weekend
        assert c.begin.min().date() == date.today() - timedelta(days=5)
        assert c.end.max().date() <= date.today() - timedelta(days=4)

        c['dt'] = c['end'] - c['begin']
        # sometimes last\first interval can be a little bit smaller
        assert (c['dt'] >= timedelta(minutes=9, seconds=50)).min()

        c = mc.candles(
            'stock',
            'shares',
            'SBER',
            _from=date.today() - timedelta(days=12),
            till=date.today() - timedelta(days=10),
            interval='60'
        )

        assert len(c.index) > 1
        assert c.begin.min().date() == date.today() - timedelta(days=12)
        assert c.end.max().date() <= date.today() - timedelta(days=10)

        c['dt'] = c['end'] - c['begin']
        assert (c['dt'] >= timedelta(minutes=59, seconds=50)).min()

    def test_members(self):
        m = mc.get_members()
        assert len(m)


if __name__ == '__main__':
    main()
