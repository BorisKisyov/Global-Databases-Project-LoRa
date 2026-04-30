import json
from dataclasses import dataclass
from typing import Dict

from Crypto.Cipher import AES
from Crypto.Hash import CMAC
from Crypto.Random import get_random_bytes


@dataclass
class SecureFrame:
    device_id: str
    counter: int
    nonce: bytes
    ciphertext: bytes
    mic: bytes

    def to_dict(self) -> Dict[str, str | int]:
        return {
            "device_id": self.device_id,
            "counter": self.counter,
            "nonce": self.nonce.hex(),
            "ciphertext": self.ciphertext.hex(),
            "mic": self.mic.hex(),
        }


class SecureRawLoRaNode:
    def __init__(self, device_id: str, aes_key: bytes):
        self.device_id = device_id
        self.aes_key = aes_key
        self.counter = 0

    def build_frame(self, plaintext: bytes) -> SecureFrame:
        self.counter += 1
        nonce = get_random_bytes(8)
        ctr_nonce = nonce + self.counter.to_bytes(8, "big")
        cipher = AES.new(self.aes_key, AES.MODE_CTR, nonce=ctr_nonce[:8])
        ciphertext = cipher.encrypt(plaintext)

        header = self.device_id.encode() + self.counter.to_bytes(8, "big") + nonce + ciphertext
        cmac = CMAC.new(self.aes_key, ciphermod=AES)
        cmac.update(header)
        mic = cmac.digest()[:8]
        return SecureFrame(self.device_id, self.counter, nonce, ciphertext, mic)


class SecureRawLoRaGateway:
    def __init__(self, key_registry: Dict[str, bytes]):
        self.key_registry = key_registry
        self.last_counter: Dict[str, int] = {}

    def verify_frame(self, frame: SecureFrame) -> Dict[str, str | int | bool]:
        key = self.key_registry.get(frame.device_id)
        if not key:
            return {"accepted": False, "reason": "unknown_device"}

        header = frame.device_id.encode() + frame.counter.to_bytes(8, "big") + frame.nonce + frame.ciphertext
        cmac = CMAC.new(key, ciphermod=AES)
        cmac.update(header)
        expected_mic = cmac.digest()[:8]
        if expected_mic != frame.mic:
            return {"accepted": False, "reason": "mic_invalid"}

        previous = self.last_counter.get(frame.device_id, 0)
        if frame.counter <= previous:
            return {"accepted": False, "reason": "replay_detected", "last_counter": previous}

        ctr_nonce = frame.nonce + frame.counter.to_bytes(8, "big")
        cipher = AES.new(key, AES.MODE_CTR, nonce=ctr_nonce[:8])
        plaintext = cipher.decrypt(frame.ciphertext)

        self.last_counter[frame.device_id] = frame.counter
        return {
            "accepted": True,
            "reason": "ok",
            "counter": frame.counter,
            "plaintext": plaintext.decode(errors="replace"),
        }


def run_demo() -> list[dict]:
    key = bytes.fromhex("00112233445566778899aabbccddeeff")
    node = SecureRawLoRaNode("demo-node-01", key)
    gateway = SecureRawLoRaGateway({"demo-node-01": key})

    valid = node.build_frame(b"CO2=1000;TEMP=20.0;RH=50")
    tampered = SecureFrame(
        device_id=valid.device_id,
        counter=valid.counter,
        nonce=valid.nonce,
        ciphertext=valid.ciphertext[:-1] + bytes([valid.ciphertext[-1] ^ 0xFF]),
        mic=valid.mic,
    )
    replay = SecureFrame(
        device_id=valid.device_id,
        counter=valid.counter,
        nonce=valid.nonce,
        ciphertext=valid.ciphertext,
        mic=valid.mic,
    )

    results = [
        {"test": "valid_frame", "frame": valid.to_dict(), "result": gateway.verify_frame(valid)},
        {"test": "tampered_ciphertext", "frame": tampered.to_dict(), "result": gateway.verify_frame(tampered)},
        {"test": "replay_attack", "frame": replay.to_dict(), "result": gateway.verify_frame(replay)},
    ]
    return results


if __name__ == "__main__":
    print(json.dumps(run_demo(), indent=2))
