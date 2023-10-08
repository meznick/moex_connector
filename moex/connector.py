from datetime import date, timedelta
from enum import Enum
from functools import wraps

from requests import Session
from typing import Optional
from xml.etree import ElementTree
import pandas as pd


class ConnectorModes(Enum):
    DATAFRAME = 'df'
    JSON = 'json'


class TransformTypes(Enum):
    DEFAULT = 'default'
    SECURITY = 'security'


SELECTED_MODE = ConnectorModes.JSON


def boilerplate_decorator(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        response = func(*args, params=kwargs)
        if response.status_code != 200:
            raise MoexConnector.APIException(
                f"Bad response: [{response.status_code}] {response.text[:100]}."
            )

        global SELECTED_MODE
        func_name = func.__name__
        transform_map = args[0].response_transform_map
        return transform_result(
            response.text,
            SELECTED_MODE,
            transform_map[func_name] if func_name in transform_map.keys() else TransformTypes.DEFAULT
        )
    return wrapper


def transform_result(
    text: str,
    output_format: Optional[ConnectorModes],
    transform_type: Optional[TransformTypes] = TransformTypes.DEFAULT,
):
    transformed = MoexConnector.generate_dataframe_from_tree(
        ElementTree.fromstring(text)
    )

    if transform_type == TransformTypes.SECURITY:
        transformed.index = transformed.name
        transformed = transformed[['value']].transpose()

    rename_dict = {
        col: col.lower() for col in transformed.columns
    }
    transformed.rename(columns=rename_dict, inplace=True)

    if output_format == ConnectorModes.JSON:
        transformed = transformed.to_json(orient='records')

    return transformed


class MoexConnector(Session):
    class APIException(Exception):
        pass

    _base_url = "https://iss.moex.com/iss"

    _DTYPE_MAP = {
        'int64': 'int64',
        'int32': 'int32',
        'string': 'string',
        'datetime': 'datetime64[ns]',
        'double': 'float64',
        'date': 'string'
    }

    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, 'instance'):
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, connector_mode: Optional[ConnectorModes] = None):
        super().__init__()
        global SELECTED_MODE
        if connector_mode:
            SELECTED_MODE = connector_mode
        self.response_transform_map = {
            self.security.__name__: TransformTypes.SECURITY,
        }

    @classmethod
    def generate_dataframe_from_tree(cls, xml_tree: ElementTree) -> pd.DataFrame:
        column_types = {
            column.get('name'): cls._DTYPE_MAP[column.get('type')] for column in xml_tree[0][0][0]
        }
        df = pd.DataFrame({
            name: [
                row.get(name) for row in xml_tree[0][1]
            ] for name in column_types.keys()
        })
        df = df.astype(column_types)
        return df

    @boilerplate_decorator
    def securities(
        self,
        q: Optional[str] = None,
        lang: Optional[str] = None,
        engine: Optional[str] = None,
        is_trading: Optional[bool] = None,
        market: Optional[str] = None,
        group_by: Optional[str] = None,
        group_by_filter: Optional[str] = None,
        limit: int = 100,
        start: int = 0,
        params: dict = None
    ):
        """
        List of securities traded on the Moscow Stock Exchange.
        MOEX doc ref: https://iss.moex.com/iss/reference/5
        """
        if not params:
            params = dict()
            params['q'] = q
            params['lang'] = lang
            params['engine'] = engine
            params['is_trading'] = is_trading
            params['market'] = market
            params['group_by'] = group_by
            params['group_by_filter'] = group_by_filter
            params['limit'] = limit
            params['start'] = start
        return self.get(f"{self._base_url}/securities", params=params)

    @boilerplate_decorator
    def security(
        self,
        ticker: str,
        lang: Optional[str] = None,
        start: int = 0,
        params: dict = None
    ):
        """
        Get instrument specification. For example: https://iss.moex.com/iss/securities/IMOEX.xml.
        MOEX doc ref: https://iss.moex.com/iss/reference/13
        """
        if not params:
            params = dict()
            params['lang'] = lang
            params['start'] = start
        return self.get(f"{self._base_url}/securities/{ticker}", params=params)

    @boilerplate_decorator
    def sec_indices(
        self,
        ticker: str,
        lang: Optional[str] = None,
        only_actual: Optional[int] = 0,
        params: dict = None
    ):
        """
        List of indexes in which the security is included.
        MOEX doc ref: https://iss.moex.com/iss/reference/160
        """
        if not params:
            params = dict()
            params['lang'] = lang
            params['only_actual'] = only_actual
        return self.get(f"{self._base_url}/securities/{ticker}/indices", params=params)

    @boilerplate_decorator
    def sitenews(
        self,
        start: int = 0,
        lang: str = 'ru',
        params: dict = None
    ):
        """
        Exchange news.
        MOEX doc ref: https://iss.moex.com/iss/reference/191
        """
        if not params:
            params = dict()
            params['lang'] = lang
            params['start'] = start
        return self.get(f"{self._base_url}/sitenews", params=params)

    @boilerplate_decorator
    def events(
        self,
        start: int = 0,
        lang: str = 'ru',
        params: dict = None
    ):
        """
        Exchange events.
        MOEX doc ref: https://iss.moex.com/iss/reference/193
        """
        if not params:
            params = dict()
            params['lang'] = lang
            params['start'] = start
        return self.get(f"{self._base_url}/events", params=params)

    @boilerplate_decorator
    def engines(
        self,
        lang: str = 'ru',
        params: dict = None
    ):
        """
        Get available trading systems. For example: https://iss.moex.com/iss/engines.xml.
        MOEX doc ref: https://iss.moex.com/iss/reference/40
        """
        if not params:
            params = dict()
            params['lang'] = lang
        return self.get(f"{self._base_url}/engines", params=params)

    @boilerplate_decorator
    def markets(
        self,
        engine: str,
        lang: str = 'ru',
        params: dict = None
    ):
        """
        Get a list of markets of the trading system.
        For example: https://iss.moex.com/iss/engines/stock/markets.xml
        MOEX doc ref: https://iss.moex.com/iss/reference/42
        """
        if not params:
            params = dict()
            params['lang'] = lang
        return self.get(f"{self._base_url}/engines/{engine}/markets", params=params)

    @boilerplate_decorator
    def boards(
            self,
            engine: str,
            market: str,
            lang: str = 'ru',
            params: dict = None
    ):
        """
        Retrieve a directory of market trading modes.
        For example: https://iss.moex.com/iss/engines/stock/markets/shares/boards.xml.
        MOEX doc ref: https://iss.moex.com/iss/reference/43
        """
        if not params:
            params = dict()
            params['lang'] = lang
        return self.get(
            f"{self._base_url}/engines/{engine}/markets/{market}/boards",
            params=params
        )

    @boilerplate_decorator
    def candles(
        self,
        engine: str,
        market: str,
        security: str,
        start: Optional[int] = 0,
        till: Optional[date] = date.today() - timedelta(days=4),
        _from: Optional[date] = date.today() - timedelta(days=5),
        interval: Optional[int] = 10,
        params: dict = None
    ):
        """
        Get candles of the specified instrument according to the default mode group.
         MOEX doc ref: https://iss.moex.com/iss/reference/155
        """
        if not params:
            params = dict()
            params['from'] = _from.strftime('%Y-%m-%d')
            params['till'] = till.strftime('%Y-%m-%d')
            params['start'] = start
            params['interval'] = interval

        return self.get(
            f"{self._base_url}/engines/{engine}/markets/{market}/securities/{security}/candles",
            params=params
        )

    @boilerplate_decorator
    def other_endpoint(
        self,
        endpoint: str,
        **kwargs
    ):
        """
        Call any other API method from list https://iss.moex.com/iss/reference/.
        """
        return self.get(f"{self._base_url}/{endpoint}", params=kwargs['params'])
