# -*- coding: utf-8 -*-
import datetime as dt
import importlib.util
from pathlib import Path
import tempfile
import unittest
from unittest import mock


MODULE_PATH = (
    Path(__file__).resolve().parents[1]
    / "strategies"
    / "潮汐量化-ETF稳健折价猎手-QMT.py"
)
HAS_LOCAL_STRATEGY = MODULE_PATH.exists()
if HAS_LOCAL_STRATEGY:
    SPEC = importlib.util.spec_from_file_location("qmt_builtin_discount_hunter", str(MODULE_PATH))
    strategy = importlib.util.module_from_spec(SPEC)
    SPEC.loader.exec_module(strategy)
else:
    strategy = None


@unittest.skipUnless(HAS_LOCAL_STRATEGY, "requires ignored local strategy deployment copy")
class CandidateSelectionTests(unittest.TestCase):
    def test_candidate_pool_keeps_only_domestic_liquid_etfs_with_nav(self):
        instruments = [
            {"code": "510001.SH", "name": "境内ETF"},
            {"code": "510002.SH", "name": "跨境ETF"},
            {"code": "510003.SH", "name": "高流动ETF"},
            {"code": "510004.SH", "name": "低流动ETF"},
            {"code": "510005.SH", "name": "无净值ETF"},
        ]
        amounts = {
            "510001.SH": 5_000_000,
            "510002.SH": 20_000_000,
            "510003.SH": 100_000_000,
            "510004.SH": 4_999_999,
            "510005.SH": 8_000_000,
        }
        etf_infos = {
            "510001.SH": {"nav": 1.0},
            "510002.SH": {"nav": 1.0},
            "510003.SH": {"nav": 1.0},
            "510004.SH": {"nav": 1.0},
            "510005.SH": {},
        }

        pool = strategy.filter_candidate_pool(instruments, amounts, etf_infos)

        self.assertEqual(
            pool,
            {"510001.SH": {"unit_net": 1.0, "money": 5_000_000.0}},
        )

    def test_raw_signal_selects_deepest_tradeable_discount(self):
        pool = {
            "510001.SH": {"unit_net": 1.0, "money": 6_000_000},
            "510002.SH": {"unit_net": 1.0, "money": 7_000_000},
            "510003.SH": {"unit_net": 1.0, "money": 8_000_000},
        }
        ticks = {
            "510001.SH": {"open": 0.985, "openInt": 13},
            "510002.SH": {"open": 0.970, "openInt": 13},
            "510003.SH": {"open": 0.950, "openInt": 1},
        }

        selected = strategy.select_raw_candidates(pool, ticks)

        self.assertEqual([item["code"] for item in selected], ["510002.SH"])
        self.assertAlmostEqual(selected[0]["premium"], -3.0)

    def test_convergence_formula_preserves_current_direction(self):
        selected = [{
            "code": "510001.SH",
            "unit_net": 1.0,
            "open_price": 0.98,
            "premium": -2.0,
        }]

        deepened = strategy.validate_signal_candidates(
            selected, {"510001.SH": {"lastPrice": 0.97, "openInt": 13}}
        )
        recovered = strategy.validate_signal_candidates(
            selected, {"510001.SH": {"lastPrice": 0.99, "openInt": 13}}
        )

        self.assertEqual(deepened, [])
        self.assertEqual([item["code"] for item in recovered], ["510001.SH"])

    def test_missing_history_is_data_error_not_valid_empty_clear_signal(self):
        class FakeContext(object):
            def get_stock_list_in_sector(self, name):
                return ["510001.SH"]

            def get_instrument_detail(self, code, full):
                return {"InstrumentName": "境内ETF"}

            def get_trading_dates(self, *args):
                return ["20260710"]

            def get_market_data_ex(self, *args, **kwargs):
                return {}

        runtime = strategy.StrategyRuntime()
        with mock.patch.object(strategy, "RUNTIME", runtime), \
                mock.patch.object(strategy, "_save_runtime", return_value=None), \
                mock.patch.dict(strategy.__dict__, {
                    "get_etf_info": lambda code: {"nav": 1.0},
                }, clear=False):
            strategy.prepare_candidate_pool(FakeContext())

        self.assertEqual(runtime.candidate_state, "DATA_ERROR")

    def test_tick_without_provable_current_date_is_rejected(self):
        class FakeContext(object):
            def get_full_tick(self, codes):
                return {code: {"lastPrice": 1.0} for code in codes}

        with self.assertRaises(strategy.QmtDataError):
            strategy._get_ticks(FakeContext(), ["510001.SH"])

    def test_tiny_etf_nav_universe_is_rejected_as_partial_data(self):
        with self.assertRaises(strategy.QmtDataError):
            strategy._validate_etf_universe_count(3, 0)

    def test_all_zero_daily_amounts_are_rejected_as_broken_snapshot(self):
        with self.assertRaises(strategy.QmtDataError):
            strategy._validate_amount_coverage(
                ["510001.SH", "510002.SH"],
                {"510001.SH": 0.0, "510002.SH": 0.0},
            )


@unittest.skipUnless(HAS_LOCAL_STRATEGY, "requires ignored local strategy deployment copy")
class PositionSizingTests(unittest.TestCase):
    def test_target_value_below_one_hundred_cash_keeps_selected_position_unchanged(self):
        candidates = [{"code": "510001.SH", "premium": -2.0}]
        ticks = {"510001.SH": {"lastPrice": 1.0, "openInt": 13}}

        targets = strategy.calculate_target_values(
            candidates, available_cash=50.0, total_value=100_000.0, ticks=ticks
        )

        self.assertEqual(targets, {})

    def test_target_value_preserves_one_lot_floor_after_cash_guard(self):
        candidates = [{"code": "510001.SH", "premium": -2.0}]
        ticks = {"510001.SH": {"lastPrice": 2.0, "openInt": 13}}

        targets = strategy.calculate_target_values(
            candidates, available_cash=150.0, total_value=100_000.0, ticks=ticks
        )

        self.assertEqual(targets, {"510001.SH": 200.0})

    def test_target_delta_is_lot_rounded_and_can_reduce_selected_holding(self):
        self.assertEqual(
            strategy.calculate_target_delta(1_000.0, 1.0, 300),
            {"side": "buy", "quantity": 700, "target_quantity": 1000},
        )
        self.assertEqual(
            strategy.calculate_target_delta(300.0, 1.0, 500),
            {"side": "sell", "quantity": 200, "target_quantity": 300},
        )
        self.assertIsNone(strategy.calculate_target_delta(300.0, 1.0, 300))

    def test_empty_final_candidates_builds_clear_plan(self):
        positions = {
            "510001.SH": {"volume": 500, "can_use": 500},
            "510002.SH": {"volume": 200, "can_use": 100},
        }

        plan = strategy.build_rebalance_intents([], {}, positions, {})

        self.assertEqual(
            [(item["code"], item["side"], item["quantity"]) for item in plan],
            [("510001.SH", "sell", 500), ("510002.SH", "sell", 100)],
        )


@unittest.skipUnless(HAS_LOCAL_STRATEGY, "requires ignored local strategy deployment copy")
class RiskAndPricingTests(unittest.TestCase):
    def test_risk_exit_order_is_stop_loss_then_stop_win_then_days(self):
        today = dt.date(2026, 7, 11)
        self.assertEqual(
            strategy.risk_exit_reason(9.4, 10.0, today - dt.timedelta(days=1), today),
            "stop_loss",
        )
        self.assertEqual(
            strategy.risk_exit_reason(11.0, 10.0, today - dt.timedelta(days=3), today),
            "stop_win",
        )
        self.assertEqual(
            strategy.risk_exit_reason(10.0, 10.0, today - dt.timedelta(days=2), today),
            "max_hold_days",
        )

    def test_book_price_uses_three_etf_ticks_and_deviation_guard(self):
        tick = {
            "lastPrice": 1.0,
            "askPrice": [1.001],
            "bidPrice": [0.999],
        }
        self.assertEqual(
            strategy.calculate_order_price("510001.SH", "buy", 1.0, tick),
            1.004,
        )
        self.assertEqual(
            strategy.calculate_order_price("510001.SH", "sell", 1.0, tick),
            0.996,
        )
        with self.assertRaises(strategy.PriceDeviationError):
            strategy.calculate_order_price(
                "510001.SH",
                "buy",
                1.0,
                {"lastPrice": 1.03, "askPrice": [1.03], "bidPrice": [1.029]},
            )


@unittest.skipUnless(HAS_LOCAL_STRATEGY, "requires ignored local strategy deployment copy")
class ExecutionStateTests(unittest.TestCase):
    def test_sell_to_buy_barrier_waits_for_sell_and_recomputes_cash(self):
        runtime = strategy.StrategyRuntime()
        runtime.begin_rebalance(
            trading_date="20260711",
            sell_intents=[{"code": "510001.SH", "side": "sell", "quantity": 500}],
            buy_candidates=[{"code": "510002.SH", "premium": -2.0}],
        )

        self.assertEqual(runtime.plan_state, "SELLING")
        self.assertEqual(runtime.pop_next_intent()["side"], "sell")
        self.assertIsNone(runtime.pop_next_intent())

        runtime.finish_active_intent()
        runtime.mark_sell_positions_synced()
        runtime.start_buying(
            available_cash=1_000.0,
            total_value=1_000.0,
            ticks={"510002.SH": {"lastPrice": 1.0, "openInt": 13}},
            positions={},
        )

        self.assertEqual(runtime.plan_state, "BUYING")
        intent = runtime.pop_next_intent()
        self.assertEqual((intent["code"], intent["side"], intent["quantity"]), ("510002.SH", "buy", 1000))
        self.assertIsNone(runtime.pop_next_intent())

    def test_stage_is_idempotent_per_trading_day(self):
        runtime = strategy.StrategyRuntime()
        self.assertTrue(runtime.claim_stage("20260711", "prepare"))
        self.assertFalse(runtime.claim_stage("20260711", "prepare"))
        self.assertTrue(runtime.claim_stage("20260712", "prepare"))

    def test_runtime_account_binding_rejects_cross_account_state(self):
        runtime = strategy.StrategyRuntime()
        self.assertTrue(runtime.bind_account("SIM001", "STOCK"))
        self.assertFalse(runtime.bind_account("SIM002", "STOCK"))
        self.assertEqual(runtime.plan_state, "HALTED")

    def test_failed_required_sell_halts_before_buy_phase(self):
        runtime = strategy.StrategyRuntime()
        runtime.begin_rebalance(
            "20260711",
            [{"code": "510001.SH", "side": "sell", "quantity": 500}],
            [{"code": "510002.SH", "premium": -2.0}],
        )
        runtime.pop_next_intent()

        runtime.finish_active_intent("REJECTED")

        self.assertEqual(runtime.plan_state, "HALTED")

    def test_full_attempt_with_intent_remaining_is_retried(self):
        runtime = strategy.StrategyRuntime()
        runtime.plan_state = "BUYING"
        runtime.active_intent = strategy._normalize_intent({
            "code": "510001.SH",
            "side": "buy",
            "quantity": 1000,
            "effective_quantity": 1000,
            "filled_quantity": 300,
            "attempt": 2,
            "attempt_filled": 200,
            "order_status": 56,
            "started_at": 1.0,
            "state": "SUBMITTED",
        }, 1)

        with mock.patch.object(strategy, "RUNTIME", runtime), \
                mock.patch.object(strategy, "_save_runtime", return_value=None), \
                mock.patch("time.time", return_value=2.0):
            strategy._finish_terminal_attempt()

        self.assertIsNotNone(runtime.active_intent)
        self.assertEqual(runtime.active_intent["state"], "NEW")
        self.assertEqual(runtime.active_intent["filled_quantity"], 500)

    def test_submitted_but_invisible_order_is_not_submitted_twice(self):
        class FakeContext(object):
            def get_full_tick(self, codes):
                return {
                    code: {
                        "lastPrice": 1.0,
                        "askPrice": [1.001],
                        "bidPrice": [0.999],
                        "openInt": 13,
                        "timetag": dt.datetime.now().strftime("%Y%m%d %H:%M:%S"),
                    }
                    for code in codes
                }

        submitted = []

        def fake_trade_detail(account_id, account_type, data_type, *args):
            if data_type == "ACCOUNT":
                return [type("Account", (), {
                    "m_dAvailable": 1_000.0,
                    "m_dBalance": 1_000.0,
                    "m_Enable": True,
                })()]
            return []

        def fake_passorder(*args):
            submitted.append(args)

        runtime = strategy.StrategyRuntime()
        runtime.begin_rebalance(
            "20260711",
            [],
            [{"code": "510001.SH", "premium": -2.0}],
        )

        with mock.patch.object(strategy, "RUNTIME", runtime), \
                mock.patch.object(strategy, "_save_runtime", return_value=None), \
                mock.patch.dict(strategy.__dict__, {
                    "account": "SIM001",
                    "accountType": "STOCK",
                    "get_trade_detail_data": fake_trade_detail,
                    "passorder": fake_passorder,
                }, clear=False):
            strategy._drive_plan(FakeContext())
            strategy._drive_plan(FakeContext())
            strategy._drive_plan(FakeContext())

        self.assertEqual(len(submitted), 1)
        # 目标量是 1000 股，但执行端按指定价 1.004 元重新查现金后只能下 900 股。
        self.assertEqual(submitted[0][0:7], (23, 1101, "SIM001", "510001.SH", 11, 1.004, 900))

    def test_repeated_cancel_rejection_eventually_halts_fifo(self):
        runtime = strategy.StrategyRuntime()
        runtime.plan_state = "BUYING"
        runtime.active_intent = strategy._normalize_intent({
            "code": "510001.SH",
            "side": "buy",
            "quantity": 100,
            "state": "SUBMITTED",
            "order_id": "ORDER-1",
            "remark": "TDH-TEST",
        }, 1)

        with mock.patch.object(strategy, "RUNTIME", runtime), \
                mock.patch.object(strategy, "_save_runtime", return_value=None), \
                mock.patch.dict(strategy.__dict__, {
                    "account": "SIM001",
                    "accountType": "STOCK",
                    "cancel": lambda *args: False,
                }, clear=False):
            strategy._request_cancel(object(), runtime.active_intent, 100.0)
            self.assertNotEqual(runtime.plan_state, "HALTED")
            strategy._request_cancel(object(), runtime.active_intent, 107.0)

        self.assertEqual(runtime.plan_state, "HALTED")

    def test_passorder_exception_keeps_unknown_submission_for_reconciliation(self):
        class FakeContext(object):
            def get_full_tick(self, codes):
                return {
                    code: {
                        "lastPrice": 1.0,
                        "askPrice": [1.001],
                        "bidPrice": [0.999],
                        "openInt": 13,
                        "timetag": dt.datetime.now().strftime("%Y%m%d %H:%M:%S"),
                    }
                    for code in codes
                }

        def fake_trade_detail(account_id, account_type, data_type, *args):
            if data_type == "ACCOUNT":
                return [type("Account", (), {
                    "m_dAvailable": 1_000.0,
                    "m_dBalance": 1_000.0,
                    "m_Enable": True,
                })()]
            return []

        runtime = strategy.StrategyRuntime()
        runtime.plan_state = "BUYING"
        runtime.intent_queue = [{
            "code": "510001.SH",
            "side": "buy",
            "quantity": 100,
            "reference_price": 1.0,
        }]

        with mock.patch.object(strategy, "RUNTIME", runtime), \
                mock.patch.object(strategy, "_save_runtime", return_value=None), \
                mock.patch.dict(strategy.__dict__, {
                    "account": "SIM001",
                    "accountType": "STOCK",
                    "get_trade_detail_data": fake_trade_detail,
                    "passorder": lambda *args: (_ for _ in ()).throw(RuntimeError("late client error")),
                }, clear=False):
            strategy._drive_plan(FakeContext())

        self.assertIsNotNone(runtime.active_intent)
        self.assertEqual(runtime.active_intent["state"], "SUBMIT_UNKNOWN")

    def test_busy_risk_stage_is_not_claimed_and_can_retry_in_window(self):
        runtime = strategy.StrategyRuntime()
        runtime.trading_date = "20260711"
        runtime.is_trading_day = True
        runtime.plan_state = "BUYING"
        now = dt.datetime(2026, 7, 11, 14, 25, 0)

        with mock.patch.object(strategy, "RUNTIME", runtime), \
                mock.patch.object(strategy, "_save_runtime", return_value=None):
            strategy._run_due_stages(object(), now)

        self.assertNotIn("risk", runtime.completed_stages)

    def test_wait_cash_does_not_size_buy_from_stale_pre_sell_balance(self):
        runtime = strategy.StrategyRuntime()
        runtime.plan_state = "WAIT_CASH"
        runtime.buy_candidates = [{"code": "510001.SH", "premium": -2.0}]
        runtime.cash_before_sells = 100.0
        runtime.sell_proceeds_floor = 500.0
        runtime.cash_sync_started_at = 100.0

        with mock.patch.object(strategy, "RUNTIME", runtime), \
                mock.patch.object(strategy, "_save_runtime", return_value=None), \
                mock.patch.object(strategy, "_query_account", return_value={
                    "available_cash": 100.0,
                    "total_value": 1_000.0,
                    "enabled": True,
                }), \
                mock.patch.object(strategy, "_query_positions", return_value={}), \
                mock.patch("time.time", return_value=101.0):
            strategy._drive_plan(object())

        self.assertEqual(runtime.plan_state, "WAIT_CASH")

    def test_account_lock_rejects_second_model_instance(self):
        with tempfile.TemporaryDirectory() as tmpdir, \
                mock.patch.object(strategy, "STATE_FILE_PATH", str(Path(tmpdir) / "state.json")), \
                mock.patch.dict(strategy.__dict__, {
                    "account": "SIM001",
                    "accountType": "STOCK",
                }, clear=False), \
                mock.patch.object(strategy, "_INSTANCE_LOCK_PATH", ""):
            with mock.patch.object(strategy, "_INSTANCE_TOKEN", "instance-one"):
                self.assertTrue(strategy._acquire_instance_lock())
            try:
                with mock.patch.object(strategy, "_INSTANCE_TOKEN", "instance-two"):
                    self.assertFalse(strategy._acquire_instance_lock())
            finally:
                with mock.patch.object(strategy, "_INSTANCE_TOKEN", "instance-one"):
                    strategy._release_instance_lock()


@unittest.skipUnless(HAS_LOCAL_STRATEGY, "requires ignored local strategy deployment copy")
class QmtFieldCompatibilityTests(unittest.TestCase):
    def test_total_position_cost_is_converted_to_unit_cost(self):
        position = type("Position", (), {
            "m_strInstrumentID": "510001",
            "m_strExchangeID": "SH",
            "m_nVolume": 100,
            "m_nCanUseVolume": 100,
            "m_dPositionCost": 1_000.0,
            "m_dLastPrice": 10.0,
        })()

        with mock.patch.dict(strategy.__dict__, {
                "account": "SIM001",
                "accountType": "STOCK",
                "get_trade_detail_data": lambda *args: [position],
        }, clear=False):
            positions = strategy._query_positions()

        self.assertEqual(positions["510001.SH"]["cost_price"], 10.0)

    def test_order_error_remark_is_parsed_from_passorder_arguments(self):
        args = type("PassorderArguments", (), {
            "strategyName": "TidalETFDiscount_&&&_TDH071109300001",
        })()

        self.assertEqual(
            strategy._extract_order_error_remark(args),
            "TDH071109300001",
        )


if __name__ == "__main__":
    unittest.main()
