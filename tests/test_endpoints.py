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

    def test_security(self):
        sc = mc.security('SBER')
        assert (
            len(self.SECURITY_COLUMNS) ==
            len([col for col in self.SECURITY_COLUMNS if col.upper() in sc.columns.tolist()])
        )
        assert 1 == len(sc.index)

    def test_site_news(self):
        # nws = mc.sitenews()
        pass

    def test_events(self):
        # evs = mc.events()
        pass

    def test_securities(self):
        with self.subTest(params=None):
            scs = mc.securities()
            assert 100 == len(scs)
            assert len([col for col in self.SECURITIES_COLUMNS if col in scs.columns.tolist()]) == 16
            assert len(scs.id.unique().tolist()) == len(scs)

        with self.subTest(limit=5):
            scs = mc.securities(limit=5)
            assert 5 == len(scs)

        with self.subTest(start=7, limit=10):
            # 4 overlapping securities
            scs_0 = mc.securities(start=0, limit=10)
            ids_0 = scs_0.id.unique().tolist()
            scs_1 = mc.securities(start=7, limit=10)
            ids_1 = scs_1.id.unique().tolist()
            assert len(ids_0) == len(ids_1) == 10
            assert len((set(ids_0) - set(ids_1))) == 7
            assert len((set(ids_1) - set(ids_0))) == 7

        with self.subTest(start=10):
            # 0 overlapping securities
            scs_0 = mc.securities(start=0, limit=10)
            ids_0 = scs_0.id.unique().tolist()
            scs_1 = mc.securities(start=10, limit=10)
            ids_1 = scs_1.id.unique().tolist()
            assert len(ids_0) == len(ids_1) == 10
            assert len((set(ids_0) - set(ids_1))) == 10
            assert len((set(ids_1) - set(ids_0))) == 10

    def test_other_endpoint(self):
        # ts = mc.other_endpoint('turnovers', lang='ru')
        pass


if __name__ == '__main__':
    main()
