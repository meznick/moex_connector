from enum import Enum
from functools import wraps

from requests import Session
from typing import Optional
from xml.etree import ElementTree
import pandas as pd


class ConnectorModes(Enum):
    DATAFRAME = 'df'
    JSON = 'json'


SELECTED_MODE = ConnectorModes.JSON


def boilerplate_decorator(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        response = func(*args, params=kwargs)
        if response.status_code != 200:
            raise MoexConnector.APIException(
                f"Bad response: [{response.status_code}] {response.text[:100]}."
            )
        result = MoexConnector.generate_dataframe_from_tree(
            ElementTree.fromstring(response.text)
        )
        if SELECTED_MODE == ConnectorModes.JSON:
            result = result.to_json(orient='records')
        return result
    return wrapper


class MoexConnector(Session):
    class APIException(Exception):
        pass

    base_url = "https://iss.moex.com/iss"

    DTYPE_MAP = {
        'int64': 'int64',
        'int32': 'int32',
        'string': 'string',
        'datetime': 'datetime64[ns]',
        'double': 'float64'
    }

    def __init__(self, connector_mode: ConnectorModes):
        super().__init__()
        global SELECTED_MODE
        SELECTED_MODE = connector_mode

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
        return self.get(f"{self.base_url}/securities", params=params)

    @boilerplate_decorator
    def security(
            self,
            ticker: str,
            lang: Optional[str] = None,
            start: int = 0,
            params: dict = None
        ):
        return self.get(f"{self.base_url}/securities/{ticker}", params=params)

    @boilerplate_decorator
    def sitenews(
            self,
            start: int = 0,
            lang: str = 'ru',
            params: dict = None
    ):
        return self.get(f"{self.base_url}/sitenews", params=params)

    @boilerplate_decorator
    def events(
            self,
            start: int = 0,
            lang: str = 'ru',
            params: dict = None
    ):
        return self.get(f"{self.base_url}/events", params=params)

    @boilerplate_decorator
    def other_endpoint(
            self,
            endpoint: str,
            **kwargs
    ):
        return self.get(f"{self.base_url}/{endpoint}", params=kwargs['params'])
