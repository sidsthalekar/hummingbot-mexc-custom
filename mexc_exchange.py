import asyncio
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

from bidict import bidict

from hummingbot.connector.constants import s_decimal_NaN
from hummingbot.connector.exchange.mexc import mexc_constants as CONSTANTS, mexc_utils, mexc_web_utils as web_utils
from hummingbot.connector.exchange.mexc.mexc_api_order_book_data_source import MexcAPIOrderBookDataSource
from hummingbot.connector.exchange.mexc.mexc_api_user_stream_data_source import MexcAPIUserStreamDataSource
from hummingbot.connector.exchange.mexc.mexc_auth import MexcAuth
from hummingbot.connector.exchange_py_base import ExchangePyBase
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.connector.utils import TradeFillOrderDetails, combine_to_hb_trading_pair
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderUpdate, TradeUpdate
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.data_type.trade_fee import DeductedFromReturnsTradeFee, TokenAmount, TradeFeeBase
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.event.events import MarketEvent, OrderFilledEvent
from hummingbot.core.utils.async_utils import safe_gather
from hummingbot.core.web_assistant.connections.data_types import RESTMethod
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory

if TYPE_CHECKING:
    from hummingbot.client.config.config_helpers import ClientConfigAdapter


class MexcExchange(ExchangePyBase):
    UPDATE_ORDER_STATUS_MIN_INTERVAL = 10.0

    web_utils = web_utils

    def __init__(self,
                 client_config_map: "ClientConfigAdapter",
                 mexc_api_key: str,
                 mexc_api_secret: str,
                 trading_pairs: Optional[List[str]] = None,
                 trading_required: bool = True,
                 domain: str = CONSTANTS.DEFAULT_DOMAIN,
                 ):
        self.api_key = mexc_api_key
        self.secret_key = mexc_api_secret
        self._domain = domain
        self._trading_required = trading_required
        self._trading_pairs = trading_pairs
        self._last_trades_poll_mexc_timestamp = 1.0
        super().__init__(client_config_map)

    @property
    def name(self) -> str:
        return "mexc"

    @property
    def authenticator(self):
        return MexcAuth(
            api_key=self.api_key,
            secret_key=self.secret_key,
            time_provider=self._time_synchronizer)

    @property
    def trading_pairs(self):
        return self._trading_pairs

    @property
    def rate_limits_rules(self):
        return CONSTANTS.RATE_LIMITS

    @property
    def domain(self):
        return self._domain

    @property
    def client_order_id_max_length(self):
        return CONSTANTS.MAX_ORDER_ID_LEN

    @property
    def client_order_id_prefix(self):
        return CONSTANTS.HBOT_ORDER_ID_PREFIX

    @property
    def trading_rules_request_path(self):
        return CONSTANTS.EXCHANGE_INFO_PATH_URL

    @property
    def trading_pairs_request_path(self):
        return CONSTANTS.EXCHANGE_INFO_PATH_URL

    @property
    def check_network_request_path(self):
        return CONSTANTS.PING_PATH_URL

    @property
    def is_cancel_request_in_exchange_synchronous(self) -> bool:
        return True

    @property
    def is_trading_required(self) -> bool:
        return self._trading_required

    def supported_order_types(self):
        return [OrderType.LIMIT, OrderType.LIMIT_MAKER, OrderType.MARKET]

    async def _make_trading_pairs_request(self) -> Any:
        return await self._api_get(
            path_url=self.trading_pairs_request_path,
            headers={"Content-Type": "application/json"})

    async def _make_trading_rules_request(self) -> Any:
        return await self._api_get(
            path_url=self.trading_rules_request_path,
            headers={"Content-Type": "application/json"})

    async def _update_balances(self):
        account_info = await self._api_get(
            path_url=CONSTANTS.ACCOUNTS_PATH_URL,
            is_auth_required=True,
            headers={"Content-Type": "application/json"})

        balances = account_info.get("balances", [])
        for balance_entry in balances:
            asset = balance_entry["asset"]
            free = Decimal(balance_entry["free"])
            locked = Decimal(balance_entry["locked"])
            total = free + locked
            self._account_balances[asset] = total
            self._account_available_balances[asset] = free

    def _initialize_trading_pair_symbols_from_exchange_info(self, exchange_info: Dict[str, Any]):
        mapping = bidict()
        for symbol_data in exchange_info.get("symbols", []):
            if symbol_data.get("status") == "1" and "SPOT" in symbol_data.get("permissions", []):
                hb_pair = combine_to_hb_trading_pair(symbol_data["baseAsset"], symbol_data["quoteAsset"])
                mapping[symbol_data["symbol"]] = hb_pair
        self._set_trading_pair_symbol_map(mapping)
    async def _place_order(self,
                       order_id: str,
                       trading_pair: str,
                       amount: Decimal,
                       trade_type: TradeType,
                       order_type: OrderType,
                       price: Decimal,
                       **kwargs) -> Tuple[str, float]:
        order_result = None
        amount_str = f"{amount:f}"
        type_str = MexcExchange.mexc_order_type(order_type)
        side_str = CONSTANTS.SIDE_BUY if trade_type is TradeType.BUY else CONSTANTS.SIDE_SELL
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
        api_params = {"symbol": symbol,
                      "side": side_str,
                      "quantity": amount_str,
                      "type": type_str,
                      "newClientOrderId": order_id}
        if order_type.is_limit_type():
            price_str = f"{price:f}"
            api_params["price"] = price_str
            api_params["timeInForce"] = CONSTANTS.TIME_IN_FORCE_GTC
        try:
            order_result = await self._api_post(
                path_url=CONSTANTS.ORDER_PATH_URL,
                data=api_params,
                is_auth_required=True)
            o_id = str(order_result["orderId"])
            transact_time = order_result["transactTime"] * 1e-3
        except IOError as e:
            error_description = str(e)
            is_server_overloaded = ("status is 503" in error_description
                                    and "Unknown error, please check your request or try again later." in error_description)
            if is_server_overloaded:
                o_id = "UNKNOWN"
                transact_time = self._time_synchronizer.time()
            else:
                raise
        return o_id, transact_time

    async def _place_cancel(self, order_id: str, tracked_order: InFlightOrder):
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=tracked_order.trading_pair)
        api_params = {
            "symbol": symbol,
            "origClientOrderId": order_id,
        }
        cancel_result = await self._api_delete(
            path_url=CONSTANTS.ORDER_PATH_URL,
            params=api_params,
            is_auth_required=True)
        if cancel_result.get("status") == "NEW":
            return True
        return False

    async def _format_trading_rules(self, exchange_info_dict: Dict[str, Any]) -> List[TradingRule]:
        trading_pair_rules = exchange_info_dict.get("symbols", [])
        retval = []
        for rule in filter(mexc_utils.is_exchange_information_valid, trading_pair_rules):
            try:
                trading_pair = await self.trading_pair_associated_to_exchange_symbol(symbol=rule.get("symbol"))
                min_order_size = Decimal(rule.get("baseSizePrecision", "0.0001"))
                min_price_inc = Decimal(f"1e-{rule['quotePrecision']}")
                min_amount_inc = Decimal(f"1e-{rule['baseAssetPrecision']}")
                min_notional = Decimal(rule['quoteAmountPrecision'])
                retval.append(
                    TradingRule(trading_pair,
                                min_order_size=min_order_size,
                                min_price_increment=min_price_inc,
                                min_base_amount_increment=min_amount_inc,
                                min_notional_size=min_notional))

            except Exception:
                self.logger().exception(f"Error parsing the trading pair rule {rule}. Skipping.")
        return retval

    def _initialize_trading_pair_symbols_from_exchange_info(self, exchange_info: Dict[str, Any]):
        mapping = bidict()
        for symbol_data in filter(mexc_utils.is_exchange_information_valid, exchange_info.get("symbols", [])):
            base = symbol_data.get("baseAsset")
            quote = symbol_data.get("quoteAsset")
            if not base or not quote:
                continue
            try:
                hb_pair = combine_to_hb_trading_pair(base=base, quote=quote)
                mapping[symbol_data["symbol"]] = hb_pair
            except Exception:
                self.logger().exception(f"Error combining trading pair: {base}-{quote}")
        self._set_trading_pair_symbol_map(mapping)
    async def _make_trading_pairs_request(self) -> Any:
        exchange_info = await self._api_get(
            path_url=self.trading_pairs_request_path,
            headers={"Content-Type": "application/json"}
        )
        return exchange_info

    async def _make_trading_rules_request(self) -> Any:
        exchange_info = await self._api_get(
            path_url=self.trading_rules_request_path,
            headers={"Content-Type": "application/json"}
        )
        return exchange_info

    def _initialize_trading_pair_symbols_from_exchange_info(self, exchange_info: Dict[str, Any]):
        mapping = bidict()
        for symbol_data in exchange_info.get("symbols", []):
            if symbol_data.get("status") == "1":  # Only include enabled pairs
                base = symbol_data.get("baseAsset")
                quote = symbol_data.get("quoteAsset")
                if base and quote:
                    mapping[symbol_data["symbol"]] = f"{base}-{quote}"
        self._set_trading_pair_symbol_map(mapping)

    async def _format_trading_rules(self, exchange_info_dict: Dict[str, Any]) -> List[TradingRule]:
        trading_pair_rules = exchange_info_dict.get("symbols", [])
        retval = []
        for rule in trading_pair_rules:
            try:
                trading_pair = await self.trading_pair_associated_to_exchange_symbol(symbol=rule.get("symbol"))
                min_order_size = Decimal(rule.get("baseSizePrecision", "0.0001"))
                min_price_inc = Decimal(f"1e-{rule.get('quotePrecision', 6)}")
                min_amount_inc = Decimal(f"1e-{rule.get('baseAssetPrecision', 2)}")
                min_notional = Decimal(rule.get("quoteAmountPrecision", "1"))

                retval.append(
                    TradingRule(
                        trading_pair,
                        min_order_size=min_order_size,
                        min_price_increment=min_price_inc,
                        min_base_amount_increment=min_amount_inc,
                        min_notional_size=min_notional,
                    )
                )
            except Exception:
                self.logger().exception(f"Error parsing trading rule for {rule.get('symbol')}. Skipping.")

        return retval
...
    async def _all_trade_updates_for_order(self, order: InFlightOrder) -> List[TradeUpdate]:
        trade_updates = []

        if order.exchange_order_id is not None:
            exchange_order_id = order.exchange_order_id
            trading_pair = await self.exchange_symbol_associated_to_pair(trading_pair=order.trading_pair)
            all_fills_response = await self._api_get(
                path_url=CONSTANTS.MY_TRADES_PATH_URL,
                params={
                    "symbol": trading_pair,
                    "orderId": exchange_order_id
                },
                is_auth_required=True,
                limit_id=CONSTANTS.MY_TRADES_PATH_URL,
                headers={"Content-Type": "application/json"})

            for trade in all_fills_response:
                exchange_order_id = str(trade["orderId"])
                fee = TradeFeeBase.new_spot_fee(
                    fee_schema=self.trade_fee_schema(),
                    trade_type=order.trade_type,
                    percent_token=trade["commissionAsset"],
                    flat_fees=[TokenAmount(amount=Decimal(trade["commission"]), token=trade["commissionAsset"])]
                )
                trade_update = TradeUpdate(
                    trade_id=str(trade["id"]),
                    client_order_id=order.client_order_id,
                    exchange_order_id=exchange_order_id,
                    trading_pair=trading_pair,
                    fee=fee,
                    fill_base_amount=Decimal(trade["qty"]),
                    fill_quote_amount=Decimal(trade["quoteQty"]),
                    fill_price=Decimal(trade["price"]),
                    fill_timestamp=trade["time"] * 1e-3,
                )
                trade_updates.append(trade_update)

        return trade_updates

    async def _request_order_status(self, tracked_order: InFlightOrder) -> OrderUpdate:
        trading_pair = await self.exchange_symbol_associated_to_pair(trading_pair=tracked_order.trading_pair)
        updated_order_data = await self._api_get(
            path_url=CONSTANTS.ORDER_PATH_URL,
            params={
                "symbol": trading_pair,
                "origClientOrderId": tracked_order.client_order_id},
            is_auth_required=True,
            headers={"Content-Type": "application/json"})

        new_state = CONSTANTS.ORDER_STATE[updated_order_data["status"]]

        order_update = OrderUpdate(
            client_order_id=tracked_order.client_order_id,
            exchange_order_id=str(updated_order_data["orderId"]),
            trading_pair=tracked_order.trading_pair,
            update_timestamp=updated_order_data["updateTime"] * 1e-3,
            new_state=new_state,
        )

        return order_update

    async def _update_balances(self):
        local_asset_names = set(self._account_balances.keys())
        remote_asset_names = set()

        account_info = await self._api_get(
            path_url=CONSTANTS.ACCOUNTS_PATH_URL,
            is_auth_required=True,
            headers={"Content-Type": "application/json"})

        balances = account_info.get("balances", [])
        self.logger().info(f"Raw balance API response: {account_info}")

        for balance_entry in balances:
            asset_name = balance_entry["asset"]
            free_balance = Decimal(balance_entry["free"])
            total_balance = Decimal(balance_entry["free"]) + Decimal(balance_entry["locked"])
            self._account_available_balances[asset_name] = free_balance
            self._account_balances[asset_name] = total_balance
            remote_asset_names.add(asset_name)

        asset_names_to_remove = local_asset_names.difference(remote_asset_names)
        for asset_name in asset_names_to_remove:
            del self._account_available_balances[asset_name]
            del self._account_balances[asset_name]

    def _initialize_trading_pair_symbols_from_exchange_info(self, exchange_info: Dict[str, Any]):
        mapping = bidict()
        for symbol_data in exchange_info["symbols"]:
            mapping[symbol_data["symbol"]] = combine_to_hb_trading_pair(base=symbol_data["baseAsset"],
                                                                          quote=symbol_data["quoteAsset"])
        self._set_trading_pair_symbol_map(mapping)

    async def _get_last_traded_price(self, trading_pair: str) -> float:
        params = {
            "symbol": await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
        }

        resp_json = await self._api_request(
            method=RESTMethod.GET,
            path_url=CONSTANTS.TICKER_PRICE_CHANGE_PATH_URL,
            params=params,
            headers={"Content-Type": "application/json"}
        )

        return float(resp_json["lastPrice"])

    async def _make_network_check_request(self):
        await self._api_get(path_url=self.check_network_request_path, headers={"Content-Type": "application/json"})

    async def _make_trading_rules_request(self) -> Any:
        exchange_info = await self._api_get(path_url=self.trading_rules_request_path, headers={"Content-Type": "application/json"})
        return exchange_info

    async def _make_trading_pairs_request(self) -> Any:
        exchange_info = await self._api_get(path_url=self.trading_pairs_request_path, headers={"Content-Type": "application/json"})
        return exchange_info
