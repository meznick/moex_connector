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

    def test_other_endpoint(self):
        ts = mc.other_endpoint('turnovers', lang='ru')
        assert ts.shape[0] > 0


if __name__ == '__main__':
    main()
