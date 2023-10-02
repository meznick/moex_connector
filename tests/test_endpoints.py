import datetime
from unittest import TestCase, main
from moex.connector import MoexConnector, ConnectorModes

mc = MoexConnector(connector_mode=ConnectorModes.DATAFRAME)


class TestEndpoints(TestCase):
    SECURITIES_COLUMNS = [
        'id', 'secid', 'shortname', 'regnumber', 'name', 'isin', 'is_traded', 'emitent_id',
        'emitent_title', 'emitent_inn', 'emitent_okpo', 'gosreg', 'type', 'group',
        'primary_boardid', 'marketprice_boardid'
    ]
    SECURITY_COLUMNS = [
        'secid', 'name', 'shortname', 'isin', 'regnumber', 'issuesize', 'facevalue', 'faceunit',
        'issuedate', 'latname', 'listlevel', 'isqualifiedinvestors', 'morningsession',
        'eveningsession', 'typename', 'group', 'type', 'groupname', 'emitter_id'
    ]
    ENGINES_COLUMNS = ['id', 'name', 'title']
    MARKET_COLUMNS = ENGINES_COLUMNS
    BOARDS_COLUMNS = ['id', 'board_group_id', 'boardid', 'title', 'is_traded']
    SITE_NEWS_COLUMNS = ['id', 'tag', 'title', 'published_at', 'modified_at']
    EVENTS_COLUMNS = SITE_NEWS_COLUMNS
    CANDLES_COLUMNS = ['open', 'close', 'high', 'low', 'value', 'volume', 'begin', 'end']

    # test
    # def test_security(self):
    #     sc = mc.security('SBER')
    #     assert (
    #         len(self.SECURITY_COLUMNS) ==
    #         len([col for col in self.SECURITY_COLUMNS if col in sc.columns.tolist()])
    #     )
    #     assert 1 == len(sc.index)

    def test_site_news(self):
        sn = mc.sitenews()
        assert (
            len(self.SITE_NEWS_COLUMNS) ==
            len([col for col in self.SITE_NEWS_COLUMNS if col in sn.columns.tolist()])
        )

    def test_events(self):
        e = mc.sitenews()
        assert (
                len(self.EVENTS_COLUMNS) ==
                len([col for col in self.EVENTS_COLUMNS if col in e.columns.tolist()])
        )

    # def test_securities(self):
    #     with self.subTest(params=None):
    #         scs = mc.securities()
    #         assert 100 == len(scs)
    #         assert (
    #             len([col for col in self.SECURITIES_COLUMNS if col in scs.columns.tolist()]) ==
    #             len(self.SECURITIES_COLUMNS)
    #         )
    #         assert len(scs.id.unique().tolist()) == len(scs)
    #
    #     with self.subTest(limit=5):
    #         scs = mc.securities(limit=5)
    #         assert 5 == len(scs)
    #
    #     with self.subTest(start=7, limit=10):
    #         # 4 overlapping securities
    #         scs_0 = mc.securities(start=0, limit=10)
    #         ids_0 = scs_0.id.unique().tolist()
    #         scs_1 = mc.securities(start=7, limit=10)
    #         ids_1 = scs_1.id.unique().tolist()
    #         assert len(ids_0) == len(ids_1) == 10
    #         assert len((set(ids_0) - set(ids_1))) == 7
    #         assert len((set(ids_1) - set(ids_0))) == 7
    #
    #     with self.subTest(start=10):
    #         # 0 overlapping securities
    #         scs_0 = mc.securities(start=0, limit=10)
    #         ids_0 = scs_0.id.unique().tolist()
    #         scs_1 = mc.securities(start=10, limit=10)
    #         ids_1 = scs_1.id.unique().tolist()
    #         assert len(ids_0) == len(ids_1) == 10
    #         assert len((set(ids_0) - set(ids_1))) == 10
    #         assert len((set(ids_1) - set(ids_0))) == 10

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
        # ts = mc.other_endpoint('turnovers', lang='ru')
        pass


if __name__ == '__main__':
    main()
