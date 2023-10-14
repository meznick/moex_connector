# MOEX Connector
[![Coverage Status](https://coveralls.io/repos/github/meznick/moex_connector/badge.svg?branch=main)](https://coveralls.io/github/meznick/moex_connector?branch=main)

```
Информационно-статистический сервер Московской Биржи (ИСС / ISS) функционирует в рамках 
программного комплекса Интернет-представительства Московской Биржи и служит для предоставления 
клиентским приложениям данных с рынков Московской Биржи.

В рамках интерфейса доступны следующие типы информации: статические данные о рынках 
(режимы торгов и их группы, финансовые инструменты и их описание), данные для построения 
графиков ("свечей"), сделки (анонимно), котировки, исторические данные, различные метаданные.

Аналогично продукту MOEX Trade INFO, который также работает через ИСС, данные могут 
предоставляться или по подписке в режиме реального времени или в свободном доступе 
(без авторизации, но с задержкой).
```
[Линк на документацию](https://iss.moex.com/iss/reference/)

## Установка
```shell
git clone git@github.com:meznick/moex_connector.git
cd moex_connector
pip install .
``` 

## Использование
Создаем новый экземпляр:
```python
from moex.connector import MoexConnector
mc = MoexConnector()
```
Так же можно передать в конструктор вид возвращаемых данных:
```python
from moex.connector import ConnectorModes, MoexConnector
MoexConnector(ConnectorModes.JSON)
MoexConnector(ConnectorModes.DATAFRAME)
```
Методы стоит вызывать следующим образом:
```python
from moex.connector import MoexConnector

mc = MoexConnector()

ticker = 'SBER'

# вернет json
mc.sec_indices(
    # если в документации (ссылка в начале) параметр есть в эндпоинте,
    # то его передаем как позиционный параметр
    ticker,
    # остальные параметры передаем как параметры GET-запроса, через
    # именованные параметры, либо словарем через kwargs
    lang='ru',
    only_actual=1
)

# для эндпоинтов которые еще красиво не имплементированы можно воспользоваться
mc.other_endpoint(
    endpoint='statistics/engines/stock/markets/index/rusfar',
    kwargs={
        'date': '2024-01-01'
    }
)
```

## Реализовано
- обращение к любому из методов, результат в виде pandas df, либо json.

## Планируется
- Доделать аннотации параметров ко всем методам
- более умное форматирование возвращаемых данных
- Добор всей информации возвращаемой некоторыми методами (забыл каких)
- ? асинхронность
