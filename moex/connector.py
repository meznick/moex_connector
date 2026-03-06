import logging
from datetime import date, timedelta
from enum import Enum
from functools import wraps
from typing import Optional
from xml.etree import ElementTree

import pandas as pd
from requests import Session

from moex import TRANSPOSE_SETTING, ConnectorModes, OUTPUT_MODE


def process_call_decorator(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        response = func(*args, **kwargs)
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

    _base_url = "https://iss.moex.com/iss"

    _dtype_map = {
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

    def __init__(self, output_mode: Optional[ConnectorModes] = None):
        super().__init__()
        if output_mode:
            self.output_mode = output_mode
        else:
            self.output_mode = OUTPUT_MODE

    @classmethod
    def response_text_to_df(cls, text: str) -> pd.DataFrame:
        return cls.generate_dataframe_from_tree(ElementTree.fromstring(text))

    @classmethod
    def generate_dataframe_from_tree(cls, xml_tree: ElementTree) -> pd.DataFrame:
        try:
            metadata = xml_tree[0][0].tag == 'metadata'
        except Exception as e:
            logging.warning(e)
            metadata = False

        if metadata:
            column_types = {
                column.get('name'): cls._dtype_map[column.get('type')] for column in xml_tree[0][0][0]
            }
            df = pd.DataFrame({
                name: [
                    row.get(name) for row in xml_tree[0][1]
                ] for name in column_types.keys()
            })
            df = df.astype(column_types, errors='ignore')
        else:
            rows = [r for r in xml_tree[0]]
            data = []
            for index, row in enumerate(rows):
                data.append(
                    pd.DataFrame(row.attrib, index=[index])
                )
            df = pd.concat(data)
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
    ):
        """
        List of securities traded on the Moscow Stock Exchange.
        MOEX doc ref: https://iss.moex.com/iss/reference/5
        """
        return self.get(f"{self._base_url}/securities", params={
            'q': q,
            'lang': lang,
            'engine': engine,
            'is_trading': is_trading,
            'market': market,
            'group_by': group_by,
            'group_by_filter': group_by_filter,
            'limit': limit,
            'start': start,
        })

    @process_call_decorator
    def security(
        self,
        ticker: str,
        lang: Optional[str] = None,
        start: int = 0,
    ):
        """
        Get instrument specification. For example: https://iss.moex.com/iss/securities/IMOEX.xml.
        MOEX doc ref: https://iss.moex.com/iss/reference/13
        """
        return self.get(f"{self._base_url}/securities/{ticker}", params={
            'lang': lang,
            'start': start,
        })

    @process_call_decorator
    def sec_indices(
        self,
        ticker: str,
        lang: Optional[str] = None,
        only_actual: Optional[int] = 0,
    ):
        """
        List of indexes in which the security is included.
        MOEX doc ref: https://iss.moex.com/iss/reference/160
        """
        return self.get(f"{self._base_url}/securities/{ticker}/indices", params={
            'lang': lang,
            'only_actual': only_actual
        })

    @process_call_decorator
    def sitenews(
        self,
        start: int = 0,
        lang: str = 'ru',
    ):
        """
        Exchange news.
        MOEX doc ref: https://iss.moex.com/iss/reference/191
        """
        return self.get(f"{self._base_url}/sitenews", params={
            'lang': lang,
            'start': start,
        })

    @process_call_decorator
    def events(
        self,
        start: int = 0,
        lang: str = 'ru',
    ):
        """
        Exchange events.
        MOEX doc ref: https://iss.moex.com/iss/reference/193
        """
        return self.get(f"{self._base_url}/events", params={
            'lang': lang,
            'start': start,
        })

    @process_call_decorator
    def engines(
        self,
        lang: str = 'ru',
    ):
        """
        Get available trading systems. For example: https://iss.moex.com/iss/engines.xml.
        MOEX doc ref: https://iss.moex.com/iss/reference/40
        """
        return self.get(f"{self._base_url}/engines", params={
            'lang': lang
        })

    @process_call_decorator
    def markets(
        self,
        engine: str,
        lang: str = 'ru',
    ):
        """
        Get a list of markets of the trading system.
        For example: https://iss.moex.com/iss/engines/stock/markets.xml
        MOEX doc ref: https://iss.moex.com/iss/reference/42
        """
        return self.get(f"{self._base_url}/engines/{engine}/markets", params={
            'lang': lang
        })

    @process_call_decorator
    def boards(
            self,
            engine: str,
            market: str,
            lang: str = 'ru',
    ):
        """
        Retrieve a directory of market trading modes.
        For example: https://iss.moex.com/iss/engines/stock/markets/shares/boards.xml.
        MOEX doc ref: https://iss.moex.com/iss/reference/43
        """
        return self.get(
            f"{self._base_url}/engines/{engine}/markets/{market}/boards",
            params={
                'lang': lang
            }
        )

    @process_call_decorator
    def candles(
        self,
        engine: str,
        market: str,
        security: str,
        start: Optional[int] = 0,
        till: Optional[date] = date.today() - timedelta(days=4),
        _from: Optional[date] = date.today() - timedelta(days=5),
        interval: Optional[str] = '10',
    ):
        """
        Get candles of the specified instrument according to the default mode group.
        MOEX doc ref: https://iss.moex.com/iss/reference/155
        :param engine: can be got from engines method
        :param market: cam be got from markets method
        :param security: can be got from securities method
        :param start: number of row to start with (shift). starts from 0
        :param till: end date
        :param _from: start date
        :param interval: possible values: 1, 10, 60, 1H - need to research more
        """
        return self.get(
            f"{self._base_url}/engines/{engine}/markets/{market}/securities/{security}/candles",
            params={
                'from': _from.strftime('%Y-%m-%d'),
                'till': till.strftime('%Y-%m-%d'),
                'start': start,
                'interval': interval,
            }
        )

    @process_call_decorator
    def other_endpoint(
        self,
        endpoint: str,
        **kwargs
    ):
        """
        Call any other API method from list https://iss.moex.com/iss/reference/.
        """
        return self.get(f"{self._base_url}/{endpoint}", params=kwargs)
