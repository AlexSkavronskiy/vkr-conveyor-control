import asyncio
import socket
import struct
import time

# === Настройки ===
RECV_PORT = 5005
PACKET_TIMEOUT = 0.1  # секунд без приёма → отправить нули

id_to_port = {
    1: ["192.168.1.223", 4311],
    2: ["192.168.1.222", 4312],
    3: ["192.168.1.221", 4313],
    4: ["192.168.1.220", 4314],
    5: ["192.168.1.219", 4315],
    6: ["192.168.1.218", 4316],
    7: ["192.168.1.217", 4317],
    8: ["192.168.1.216", 4318],
    9: ["192.168.1.215", 4319],
}

ENABLE_LOG = False

def create_packet(board_id, lt, rt, lb, rb):
    def to_signed_16(val):
        if not -255 <= val <= 255:
            raise ValueError(f"Value out of range (-255 to 255): {val}")
        return val & 0xFFFF

    packet = bytearray(12)
    packet[0] = 0xAA
    packet[1] = board_id & 0xFF
    packet[2] = 0x02

    values = [lt, rt, lb, rb]
    for i, val in enumerate(values):
        signed_val = to_signed_16(val)
        packet[3 + i * 2] = (signed_val >> 8) & 0xFF
        packet[4 + i * 2] = signed_val & 0xFF

    crc = 0
    for i in range(1, 11):
        crc ^= packet[i]
    packet[11] = crc

    return packet

class UDPProtocol:
    def __init__(self):
        self.transport = None
        self.on_data = asyncio.Event()
        self.buffer = None

    def connection_made(self, transport):
        self.transport = transport

    def datagram_received(self, data, addr):
        self.buffer = (data, addr)
        self.on_data.set()

    async def recv(self):
        await self.on_data.wait()
        self.on_data.clear()
        return self.buffer

async def send_to_all_esp(values):
    for i in range(9):
        ID = i + 1
        LT, RT, LB, RB = values[i * 4:(i + 1) * 4]
        packet = create_packet(ID, LT, RT, LB, RB)
        ip, port = id_to_port[ID]

        try:
            send_sock.sendto(packet, (ip, port))
            if ENABLE_LOG:
                print(f"{ID=} {LT=} {RT=} {LB=} {RB=} sent")
        except Exception as e:
            print(f"❌ Ошибка при отправке в ячейку {ID}: {e}")

        await asyncio.sleep(0.0005)

async def main():
    print(f"🟢 UDP сервер запущен на порту {RECV_PORT}")
    loop = asyncio.get_running_loop()

    transport, protocol = await loop.create_datagram_endpoint(
        lambda: UDPProtocol(),
        local_addr=("0.0.0.0", RECV_PORT)
    )

    global send_sock
    send_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    send_sock.setblocking(False)

    last_packet_time = time.time()
    last_valid_values = [0] * 36  # стартуем с нулей

    try:
        while True:
            try:
                # Ждём пакет с таймаутом
                data_task = asyncio.create_task(protocol.recv())
                done, _ = await asyncio.wait({data_task}, timeout=PACKET_TIMEOUT)

                if data_task in done:
                    data, _ = data_task.result()
                    if len(data) == 72:
                        values = list(struct.unpack("<36h", data))
                        last_valid_values = values
                        last_packet_time = time.time()
                        await send_to_all_esp(values)
                    else:
                        print(f"⚠️ Неверный размер пакета: {len(data)} байт")

                else:
                    # Таймаут → отправим нули
                    if ENABLE_LOG:
                        print("⏱️ Таймаут приёма: отправка нулей")
                    await send_to_all_esp([0] * 36)

            except Exception as e:
                print(f"❌ Ошибка обработки: {e}")
                await asyncio.sleep(0.01)

    except asyncio.CancelledError:
        print("⛔ Цикл остановлен")
    finally:
        transport.close()
        send_sock.close()
        print("🔌 Сокеты закрыты. Программа завершена.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🛑 Прервано пользователем (Ctrl+C)")
