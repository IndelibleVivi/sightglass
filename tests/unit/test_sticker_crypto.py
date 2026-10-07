from __future__ import annotations

import unittest

from cryptography.hazmat.primitives import padding
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

from sightglass.contracts.errors import ErrorCode, SightglassError
from sightglass.resources.sticker import decrypt_wechat_sticker, derive_wechat_sticker_key


class StickerCryptoTests(unittest.TestCase):
    def setUp(self) -> None:
        self.key = bytes.fromhex("00112233445566778899aabbccddeeff")
        self.plaintext = b"GIF89a" + b"synthetic animated sticker payload"
        padder = padding.PKCS7(128).padder()
        padded = padder.update(self.plaintext) + padder.finalize()
        encryptor = Cipher(algorithms.AES(self.key), modes.CBC(self.key)).encryptor()
        self.ciphertext = encryptor.update(padded) + encryptor.finalize()

    def test_derives_the_file_xor_key_from_the_two_account_inputs(self) -> None:
        self.assertEqual(
            derive_wechat_sticker_key("synthetic-first", "synthetic-second").hex(),
            "cec6573d0f4fe112441eba61568fd3ef",
        )

    def test_decrypts_aes_cbc_with_key_as_iv_and_pkcs7_padding(self) -> None:
        self.assertEqual(
            decrypt_wechat_sticker(self.ciphertext, self.key),
            self.plaintext,
        )

    def test_rejects_invalid_key_and_ciphertext_shapes(self) -> None:
        for ciphertext, key in (
            (self.ciphertext, b"short"),
            (b"", self.key),
            (self.ciphertext[:-1], self.key),
        ):
            with self.subTest(ciphertext_size=len(ciphertext), key_size=len(key)):
                with self.assertRaises(SightglassError) as caught:
                    decrypt_wechat_sticker(ciphertext, key)
                self.assertEqual(caught.exception.code, ErrorCode.RESOURCE_DECODE_FAILED)

    def test_rejects_invalid_pkcs7_padding(self) -> None:
        corrupted = self.ciphertext[:-1] + bytes([self.ciphertext[-1] ^ 0x01])

        with self.assertRaises(SightglassError) as caught:
            decrypt_wechat_sticker(corrupted, self.key)

        self.assertEqual(caught.exception.code, ErrorCode.RESOURCE_DECODE_FAILED)


if __name__ == "__main__":
    unittest.main()
