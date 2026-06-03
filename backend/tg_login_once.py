import asyncio
import getpass
from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError
import server

async def main():
    client = TelegramClient(
        server.get_telegram_session_path(),
        int(server.TELEGRAM_API_ID),
        server.TELEGRAM_API_HASH,
    )
    await client.connect()

    if await client.is_user_authorized():
        print("ALREADY_AUTHORIZED")
        me = await client.get_me()
        print("ME =", getattr(me, "username", None) or getattr(me, "phone", None) or None)
        await client.disconnect()
        return

    sent = await client.send_code_request(server.TELEGRAM_PHONE)
    print("CODE_SENT")

    code = input("Enter Telegram code: ").strip()

    try:
        await client.sign_in(
            phone=server.TELEGRAM_PHONE,
            code=code,
            phone_code_hash=sent.phone_code_hash,
        )
    except SessionPasswordNeededError:
        password = getpass.getpass("Enter Telegram 2FA password: ")
        await client.sign_in(password=password)

    print("AUTHORIZED =", await client.is_user_authorized())
    me = await client.get_me()
    print("ME =", getattr(me, "username", None) or getattr(me, "phone", None) or None)
    await client.disconnect()

asyncio.run(main())
