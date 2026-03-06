from enum import Enum


class ConnectorModes(Enum):
    DATAFRAME = 'df'
    JSON = 'json'


OUTPUT_MODE = ConnectorModes.JSON

TRANSPOSE_SETTING = {
    'security': True,
    # other cases lead to false
}
