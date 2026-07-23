import asyncio
import json
import os
import queue
import sys
import threading
import tkinter as tk
from tkinter import ttk, messagebox
from typing import Any, Optional

from pbtlib.modules.client import AsyncTankiInstance
from pbtlib.modules.processing import AsyncAbstractProcessor
from pbtlib.modules.misc import packetManager
from pbtlib.modules.communications import LogMessage, ErrorMessage
from pbtlib.utils import Address, ReconnectionConfig

APP_TITLE = "Mine Buyer"
ENDPOINT = Address("146.59.110.146", 25565)  #protanki-server

PRICE_LOOKUP_ID = "mine"
ITEM_ID_TO_BUY = "mine_m0"

PRICE_KEY_CANDIDATES = ["next_price", "price", "cost", "base_price", "basePrice", "value"]


def resource_path(relative_path: str) -> str:
    base_path = getattr(sys, "_MEIPASS", os.path.dirname(os.path.abspath(__file__)))
    return os.path.join(base_path, relative_path)


class GuiBuyProcessor(AsyncAbstractProcessor[Any, Any]):
    def __init__(self, *args, event_queue: "queue.Queue", **kwargs):
        super().__init__(*args, **kwargs)
        self.events = event_queue

        self.login_result_event = asyncio.Event()
        self.garage_ready_event = asyncio.Event()
        self.login_success: Optional[bool] = None

        self.crystals: Optional[int] = None
        self.purchasable_items: Any = None
        self.owned_items: Any = None

        self.resolved_price: Optional[int] = None
        self.owned_count: Optional[int] = None

    @property
    def command_handlers(self):
        return {}

    def _emit(self, kind: str, **data):
        self.events.put({"kind": kind, **data})

    async def process_packets(self):
        if self.compare_packet("Login_Ready"):
            await self._send_login()
            return

        if self.compare_packet("Load_Account_Stats"):
            self.crystals = self.current_packet.object.get("crystals")
            self._emit("crystals", value=self.crystals)

        elif self.compare_packet("Load_Purchasable_Items"):
            raw = self.current_packet.object.get("json", "")
            try:
                self.purchasable_items = json.loads(raw) if raw else None
            except json.JSONDecodeError:
                self.purchasable_items = None
            self._maybe_garage_ready()
            self._sync_owned_count()

        elif self.compare_packet("Load_Owned_Garage_Items"):
            self.owned_items = self.current_packet.object.get("json")
            self._maybe_garage_ready()
            self._sync_owned_count()

    def _sync_owned_count(self):
        item = self.find_item(PRICE_LOOKUP_ID)
        if item is not None and "count" in item:
            self.owned_count = int(item["count"])
            self._emit("owned_count", value=self.owned_count)

    def _maybe_garage_ready(self):
        if self.purchasable_items is not None and self.owned_items is not None:
            self.garage_ready_event.set()

    async def _send_login(self):
        login_packet = packetManager.get_packet_by_name("Login")()
        login_packet.deimplement({
            "username": self.credentials["name"],
            "password": self.credentials["password"],
            "rememberMe": False,
        })
        await self.send_packet(login_packet)

    async def on_login(self):
        self.login_success = True
        self.login_result_event.set()
        self._emit("login", success=True)

    def _all_items(self) -> list[tuple[str, dict]]:
        found: list[tuple[str, dict]] = []

        def walk(node: Any, source: str):
            if isinstance(node, dict):
                if "id" in node:
                    found.append((source, node))
                for value in node.values():
                    walk(value, source)
            elif isinstance(node, list):
                for value in node:
                    walk(value, source)

        walk(self.purchasable_items, "purchasable_items")
        walk(self.owned_items, "owned_items")
        return found

    def find_item(self, item_id: str) -> Optional[dict]:
        for _source, item in self._all_items():
            if item.get("id") == item_id:
                return item
        return None

    async def request_garage(self):
        load_garage = packetManager.get_packet_by_name("Load_Garage")()
        await self.send_packet(load_garage)

    async def resolve_price(self) -> Optional[int]:
        price_item = self.find_item(PRICE_LOOKUP_ID)
        if price_item is None:
            return None
        price_key = next((k for k in PRICE_KEY_CANDIDATES if k in price_item), None)
        self.resolved_price = price_item.get(price_key) if price_key else None
        return self.resolved_price

    async def buy(self, count: int) -> bool:
        if self.resolved_price is None:
            raise RuntimeError("Price not resolved yet")

        cost = count * self.resolved_price
        if self.crystals is not None and self.crystals < cost:
            return False

        buy_packet = packetManager.get_packet_by_name("Buy_Multiple_Items")()
        buy_packet.deimplement({
            "item_id": ITEM_ID_TO_BUY,
            "count": count,
            "base_cost": cost,  
        })
        await self.send_packet(buy_packet)

        if self.crystals is not None:
            self.crystals -= cost
            self._emit("crystals", value=self.crystals)
        if self.owned_count is not None:
            self.owned_count += count
            self._emit("owned_count", value=self.owned_count)

        return True


class BuyMineInstance(AsyncTankiInstance):
    RECONNECTION_CONFIG = ReconnectionConfig(
        MAX_RECONNECTIONS=0,
        RECONNECTION_INTERVAL=0,
        BREAK_INTERVAL=-1,
        INSTANT_RECONNECT_INTERVAL=1,
    )

    processor: GuiBuyProcessor

    def __init__(self, *args, event_queue: "queue.Queue", **kwargs):
        self._event_queue = event_queue
        super().__init__(*args, **kwargs)

    def instantiate_processor(self) -> None:
        self.processor = GuiBuyProcessor(
            None,
            self.protection,
            self.credentials,
            self.transmit,
            event_queue=self._event_queue,
        )

    async def on_kill_instance(self, _id: int):
        pass


class NetworkThread(threading.Thread):
    def __init__(self, username: str, password: str, event_queue: "queue.Queue"):
        super().__init__(daemon=True)
        self.username = username
        self.password = password
        self.events = event_queue
        self.loop: Optional[asyncio.AbstractEventLoop] = None
        self.instance: Optional[BuyMineInstance] = None
        self._ready = threading.Event()

        self.auto_buy_enabled = threading.Event()
        self.auto_buy_count = 1
        self.auto_buy_interval_ms = 1000

    def run(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        try:
            self.loop.run_until_complete(self._main())
        except Exception as exc:
            import traceback
            self.events.put({"kind": "log", "text": f"Fatal error: {exc}"})
            self.events.put({"kind": "login", "success": False, "reason": str(exc)})
            traceback.print_exc()

    async def _transmit(self, message):
        if isinstance(message, ErrorMessage):
            self.events.put({"kind": "log", "text": f"[error] {message.error}"})

    async def _main(self):
        creds = {"name": self.username, "password": self.password, "endpoint": ENDPOINT}
        self.instance = BuyMineInstance(
            id=0,
            credentials=creds,
            transmit=self._transmit,
            handle_reconnect=lambda: None,
            on_kill_instance=lambda _id: None,
            reconnections=[],
            event_queue=self.events,
        )
        proc = self.instance.processor
        self._ready.set()

        try:
            await asyncio.wait_for(proc.login_result_event.wait(), timeout=30)
        except asyncio.TimeoutError:
            self.events.put({"kind": "login", "success": False, "reason": "timed out"})
            return

        if not proc.login_success:
            self.events.put({"kind": "login", "success": False, "reason": "rejected"})
            return

        await asyncio.sleep(3)
        await proc.request_garage()
        try:
            await asyncio.wait_for(proc.garage_ready_event.wait(), timeout=30)
        except asyncio.TimeoutError:
            self.events.put({"kind": "log", "text": "Garage never loaded."})
            return

        price = await proc.resolve_price()
        if price is None:
            self.events.put({"kind": "log", "text": f"Could not find a price for '{PRICE_LOOKUP_ID}'."})
            return

        self.events.put({"kind": "ready", "price": price, "crystals": proc.crystals,
                         "owned": proc.owned_count})

        while True:
            if self.auto_buy_enabled.is_set():
                try:
                    sent = await proc.buy(self.auto_buy_count)
                except Exception as exc:
                    self.events.put({"kind": "log", "text": f"Buy failed: {exc}"})
                    self.auto_buy_enabled.clear()
                    self.events.put({"kind": "auto_stopped", "reason": "error"})
                    continue

                if not sent:
                    self.auto_buy_enabled.clear()
                    self.events.put({"kind": "auto_stopped", "reason": "insufficient_crystals"})
                    continue

                self.events.put({"kind": "bought", "count": self.auto_buy_count})
                await asyncio.sleep(max(self.auto_buy_interval_ms, 1) / 1000)
            else:
                await asyncio.sleep(0.05)

    def buy_once(self, count: int):
        if not self.loop or not self.instance:
            return
        future = asyncio.run_coroutine_threadsafe(self.instance.processor.buy(count), self.loop)

        def _on_done(fut):
            try:
                sent = fut.result()
            except Exception as exc:
                self.events.put({"kind": "log", "text": f"Buy failed: {exc}"})
                return
            if sent:
                self.events.put({"kind": "bought", "count": count})
            else:
                self.events.put({"kind": "log", "text": "Not enough crystals for that purchase."})

        future.add_done_callback(_on_done)

    def refresh_garage(self):
        if not self.loop or not self.instance:
            return
        asyncio.run_coroutine_threadsafe(self.instance.processor.request_garage(), self.loop)

    def start_auto_buy(self, count: int, interval_ms: int):
        self.auto_buy_count = min(max(1, count), 9999)
        self.auto_buy_interval_ms = max(1, interval_ms)
        self.auto_buy_enabled.set()

    def stop_auto_buy(self):
        self.auto_buy_enabled.clear()


class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title(APP_TITLE)
        self.geometry("380x520")
        self.resizable(False, False)
        self._set_icon()

        self.event_queue: "queue.Queue" = queue.Queue()
        self.net: Optional[NetworkThread] = None
        self.owned_count = 0

        self._build_login_frame()
        self._build_main_frame()
        self.main_frame.pack_forget()

        self.after(50, self._poll_events)

    def _set_icon(self):
        try:
            self.iconbitmap(resource_path("assets/icon.ico"))
        except tk.TclError:
            try:
                self._icon_img = tk.PhotoImage(file=resource_path("assets/mine.png"))
                self.iconphoto(True, self._icon_img)
            except Exception:
                pass

    def _build_login_frame(self):
        self.login_frame = ttk.Frame(self, padding=24)
        self.login_frame.pack(expand=True, fill="both")

        ttk.Label(self.login_frame, text=APP_TITLE, font=("Segoe UI", 16, "bold")).pack(pady=(0, 16))

        ttk.Label(self.login_frame, text="Account name").pack(anchor="w")
        self.username_var = tk.StringVar()
        ttk.Entry(self.login_frame, textvariable=self.username_var).pack(fill="x", pady=(0, 10))

        ttk.Label(self.login_frame, text="Password").pack(anchor="w")
        self.password_var = tk.StringVar()
        ttk.Entry(self.login_frame, textvariable=self.password_var, show="*").pack(fill="x", pady=(0, 16))

        self.login_button = ttk.Button(self.login_frame, text="Login", command=self._on_login_click)
        self.login_button.pack(fill="x")

        self.login_status_var = tk.StringVar(value="")
        ttk.Label(self.login_frame, textvariable=self.login_status_var, foreground="#a33").pack(pady=(10, 0))

    def _on_login_click(self):
        username = self.username_var.get().strip()
        password = self.password_var.get().strip()
        if not username or not password:
            self.login_status_var.set("Enter both account name and password.")
            return

        self.login_button.config(state="disabled")
        self.login_status_var.set("Connecting...")
        self.net = NetworkThread(username, password, self.event_queue)
        self.net.start()

    def _build_main_frame(self):
        self.main_frame = ttk.Frame(self, padding=20)
        self.main_frame.pack(expand=True, fill="both")

        try:
            self._mine_img = tk.PhotoImage(file=resource_path("assets/mine.png")).subsample(2, 2)
            ttk.Label(self.main_frame, image=self._mine_img).pack(pady=(0, 8))
        except Exception:
            pass

        self.owned_var = tk.StringVar(value="Mines owned: --")
        ttk.Label(self.main_frame, textvariable=self.owned_var, font=("Segoe UI", 12, "bold")).pack()

        self.price_var = tk.StringVar(value="")
        ttk.Label(self.main_frame, textvariable=self.price_var, foreground="#555").pack(pady=(0, 4))

        self.crystals_var = tk.StringVar(value="")
        ttk.Label(self.main_frame, textvariable=self.crystals_var, foreground="#555").pack(pady=(0, 2))

        ttk.Button(self.main_frame, text="Refresh count", command=self._on_refresh).pack(fill="x", pady=(0, 14))

        ttk.Separator(self.main_frame).pack(fill="x", pady=(0, 14))

        ttk.Label(self.main_frame, text="Buy once", font=("Segoe UI", 10, "bold")).pack(anchor="w")
        row = ttk.Frame(self.main_frame)
        row.pack(fill="x", pady=(4, 14))
        ttk.Label(row, text="Count:").pack(side="left")
        self.manual_count_var = tk.StringVar(value="1")
        ttk.Entry(row, textvariable=self.manual_count_var, width=8).pack(side="left", padx=(6, 12))
        ttk.Button(row, text="Buy now", command=self._on_buy_once).pack(side="left")

        ttk.Separator(self.main_frame).pack(fill="x", pady=(0, 14))

        ttk.Label(self.main_frame, text="Auto-buy", font=("Segoe UI", 10, "bold")).pack(anchor="w")

        row2 = ttk.Frame(self.main_frame)
        row2.pack(fill="x", pady=(4, 6))
        ttk.Label(row2, text="Count per buy (max 9999):").pack(side="left")
        self.auto_count_var = tk.StringVar(value="1")
        ttk.Entry(row2, textvariable=self.auto_count_var, width=8).pack(side="left", padx=(6, 0))

        row3 = ttk.Frame(self.main_frame)
        row3.pack(fill="x", pady=(0, 10))
        ttk.Label(row3, text="Interval (ms, min 1):").pack(side="left")
        self.interval_var = tk.StringVar(value="1000")
        ttk.Entry(row3, textvariable=self.interval_var, width=8).pack(side="left", padx=(6, 0))

        row4 = ttk.Frame(self.main_frame)
        row4.pack(fill="x")
        self.start_button = ttk.Button(row4, text="Start auto-buy", command=self._on_start_auto)
        self.start_button.pack(side="left", expand=True, fill="x", padx=(0, 4))
        self.stop_button = ttk.Button(row4, text="Stop", command=self._on_stop_auto, state="disabled")
        self.stop_button.pack(side="left", expand=True, fill="x", padx=(4, 0))

        self.bought_var = tk.StringVar(value="")
        ttk.Label(self.main_frame, textvariable=self.bought_var, foreground="#555").pack(pady=(12, 0))

    def _on_refresh(self):
        if self.net:
            self.net.refresh_garage()

    def _on_buy_once(self):
        if not self.net:
            return
        try:
            count = min(max(1, int(self.manual_count_var.get())), 9999)
        except ValueError:
            messagebox.showerror(APP_TITLE, "Count must be a whole number.")
            return
        self.net.buy_once(count)

    def _on_start_auto(self):
        if not self.net:
            return
        try:
            count = int(self.auto_count_var.get())
            interval = int(self.interval_var.get())
        except ValueError:
            messagebox.showerror(APP_TITLE, "Count and interval must be whole numbers.")
            return
        if count < 1:
            messagebox.showerror(APP_TITLE, "Count per buy must be at least 1.")
            return
        if count > 9999:
            messagebox.showerror(APP_TITLE, "Count per buy cannot exceed 9999.")
            return
        if interval < 1:
            messagebox.showerror(APP_TITLE, "Interval must be at least 1 ms.")
            return
        self.net.start_auto_buy(count, interval)
        self.start_button.config(state="disabled")
        self.stop_button.config(state="normal")

    def _on_stop_auto(self):
        if self.net:
            self.net.stop_auto_buy()
        self.start_button.config(state="normal")
        self.stop_button.config(state="disabled")

    def _poll_events(self):
        try:
            while True:
                event = self.event_queue.get_nowait()
                self._handle_event(event)
        except queue.Empty:
            pass
        self.after(50, self._poll_events)

    def _handle_event(self, event: dict):
        kind = event.get("kind")

        if kind == "login":
            if event.get("success"):
                self.login_status_var.set("")
                self.login_frame.pack_forget()
                self.main_frame.pack(expand=True, fill="both")
            else:
                self.login_button.config(state="normal")
                self.login_status_var.set(f"Login failed ({event.get('reason', 'unknown')}).")

        elif kind == "ready":
            self.owned_count = event.get("owned", 0)
            self.owned_var.set(f"Mines owned: {self.owned_count}")
            self.price_var.set(f"Price: {event.get('price')} crystals each")
            self.crystals_var.set(f"Balance: {event.get('crystals')} crystals")

        elif kind == "owned_count":
            self.owned_count = event.get("value", self.owned_count)
            self.owned_var.set(f"Mines owned: {self.owned_count}")

        elif kind == "crystals":
            self.crystals_var.set(f"Balance: {event.get('value')} crystals")

        elif kind == "bought":
            self.bought_var.set(f"Sent buy request(s) for {event.get('count')} mine(s) "
                                 f"just now. Use Refresh count to re-sync with the server.")

        elif kind == "auto_stopped":
            reason = event.get("reason")
            if reason == "insufficient_crystals":
                self.bought_var.set("Auto-buy stopped: not enough crystals for another purchase.")
            else:
                self.bought_var.set("Auto-buy stopped due to an error. Check the log.")
            self.start_button.config(state="normal")
            self.stop_button.config(state="disabled")

        elif kind == "log":
            self.bought_var.set(str(event.get("text", "")))


def main():
    app = App()
    app.mainloop()


if __name__ == "__main__":
    main()
