from decimal import Decimal
from typing import Any, Dict

from pydantic import Field, SecretStr

from hummingbot.client.config.config_data_types import BaseConnectorConfigMap, ClientFieldData
from hummingbot.core.data_type.trade_fee import TradeFeeSchema

CENTRALIZED = True
EXAMPLE_PAIR = "ZRX-ETH"

DEFAULT_FEES = TradeFeeSchema(
    maker_percent_fee_decimal=Decimal("0.0005"),
    taker_percent_fee_decimal=Decimal("0.0005"),
    buy_percent_fee_deducted_from_returns=True
)

def is_exchange_information_valid(exchange_info: Dict[str, Any]) -> bool:
    return exchange_info.get("symbol", "").endswith("USDT")

class MexcConfigMap(BaseConnectorConfigMap):
    connector: str = Field(default="mexc", const=True, client_data=None)
    mexc_api_key: SecretStr = Field(
        default=...,
        client_data=ClientFieldData(
            prompt=lambda cm: "Enter your Mexc API key",
            is_secure=True,
            is_connect_key=True,
            prompt_on_new=True,
        )
    )
    mexc_api_secret: SecretStr = Field(
        default=...,
        client_data=ClientFieldData(
            prompt=lambda cm: "Enter your Mexc API secret",
            is_secure=True,
            is_connect_key=True,
            prompt_on_new=True,
        )
    )

    class Config:
        title = "mexc"

KEYS = MexcConfigMap.construct()
