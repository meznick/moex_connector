from functools import wraps
from typing import Optional
from xml.etree import ElementTree

import pandas as pd
from requests import Session

from moex import TRANSPOSE_SETTING, ConnectorModes, OUTPUT_MODE


def process_call_decorator(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        response = func(*args, params=kwargs)
        if response.status_code != 200:
            raise MoexConnector.APIException(
                f"Bad response: [{response.status_code}] {response.text[:100]}."
            )
        return transform_result(
            response.text,
            args[0].output_mode,
            transpose=TRANSPOSE_SETTING.get(func.__name__, False)
        )
    return wrapper


def transform_result(
        text: str,
        output_format: Optional[ConnectorModes] = ConnectorModes.DATAFRAME,
        transpose: bool = False,
):
    transformed = MoexConnector.generate_dataframe_from_tree(
        ElementTree.fromstring(text)
    )

    if transpose:
        transformed.index = transformed.name
        transformed = transformed.transpose()

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

    def __init__(self, output_mode: Optional[ConnectorModes] = None):
        super().__init__()
        if output_mode:
            self.output_mode = output_mode
        else:
            self.output_mode = OUTPUT_MODE

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

    @process_call_decorator
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

    @process_call_decorator
    def security(
            self,
            ticker: str,
            lang: Optional[str] = None,
            start: int = 0,
            params: dict = None
        ):
        return self.get(f"{self.base_url}/securities/{ticker}", params=params)

    @process_call_decorator
    def sec_indices(
            self,
            ticker: str,
            lang: Optional[str] = None,
            only_actual: Optional[int] = 0,
            params: dict = None
        ):
        return self.get(f"{self.base_url}/securities/{ticker}/indices", params=params)

    @process_call_decorator
    def sitenews(
            self,
            start: int = 0,
            lang: str = 'ru',
            params: dict = None
    ):
        return self.get(f"{self.base_url}/sitenews", params=params)

    @process_call_decorator
    def events(
            self,
            start: int = 0,
            lang: str = 'ru',
            params: dict = None
    ):
        return self.get(f"{self.base_url}/events", params=params)

    @process_call_decorator
    def other_endpoint(
            self,
            endpoint: str,
            **kwargs
    ):
        return self.get(f"{self.base_url}/{endpoint}", params=kwargs['params'])
