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
        return transform_result(
            response.text,
            SELECTED_MODE,
            args[0].response_transform_map[func.__name__]
        )
    return wrapper


def transform_result(
    text: str,
    output_format: Optional[ConnectorModes],
    transform_type: Optional[TransformTypes] = TransformTypes.DEFAULT,
):
    if transform_type == TransformTypes.SECURITY:
        transformed = MoexConnector.generate_dataframe_from_tree(
            ElementTree.fromstring(text)
        )
        transformed.index = transformed.name
        transformed = transformed[['value']].transpose()
    else:
        transformed = MoexConnector.generate_dataframe_from_tree(
            ElementTree.fromstring(text)
        )

    if output_format == ConnectorModes.JSON:
        transformed = transformed.to_json(orient='records')

    return transformed


class MoexConnector(Session):
    class APIException(Exception):
        pass

    base_url = "https://iss.moex.com/iss"

    DTYPE_MAP = {
        'int64': 'int64',
        'int32': 'int32',
        'string': 'string',
        'datetime': 'datetime64[ns]',
        'double': 'float64',
        'date': 'string'
    }

    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, 'instance'):
            cls.instance = super().__new__(cls)
        return cls.instance

    def __init__(self, connector_mode: Optional[ConnectorModes] = None):
        super().__init__()
        global SELECTED_MODE
        if connector_mode:
            SELECTED_MODE = connector_mode
        self.response_transform_map = {
            self.security.__name__: TransformTypes.SECURITY,
            self.securities.__name__: TransformTypes.DEFAULT,
            self.sec_indices.__name__: TransformTypes.DEFAULT,
        }

    @classmethod
    def generate_dataframe_from_tree(cls, xml_tree: ElementTree) -> pd.DataFrame:
        df = pd.DataFrame({
            column.get('name'): pd.Series(
                dtype=cls.DTYPE_MAP[column.get('type')]
            ) for column in xml_tree[0][0][0]
        })
        df = pd.concat([
            df,
            pd.DataFrame({
                name: [row.get(name) for row in xml_tree[0][1]] for name in df.columns.tolist()
            })
        ])
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
        return self.get(f"{self.base_url}/securities", params=params)

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
        return self.get(f"{self.base_url}/securities/{ticker}", params=params)

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
        return self.get(f"{self.base_url}/securities/{ticker}/indices", params=params)

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
        return self.get(f"{self.base_url}/sitenews", params=params)

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
        return self.get(f"{self.base_url}/events", params=params)

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
        return self.get(f"{self.base_url}/engines", params=params)

    @boilerplate_decorator
    def markets(
        self,
        engine: str = 'stock',
        lang: str = 'ru',
        params: dict = None
    ):
        """
        Get a list of markets of the trading system.
        For example: https://iss.moex.com/iss/engines/stock/markets.xml
        MOEX doc ref: https://iss.moex.com/iss/reference/42
        """
        return self.get(f"{self.base_url}/engines/{engine}/markets", params=params)

    @boilerplate_decorator
    def boards(
            self,
            engine: str = 'stock',
            market: str = 'shares',
            lang: str = 'ru',
            params: dict = None
    ):
        """
        Retrieve a directory of market trading modes.
        For example: https://iss.moex.com/iss/engines/stock/markets/shares/boards.xml.
        MOEX doc ref: https://iss.moex.com/iss/reference/43
        """
        return self.get(
            f"{self.base_url}/engines/{engine}/markets/{market}/boards",
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
        return self.get(f"{self.base_url}/{endpoint}", params=kwargs['params'])
