import asyncio
import logging
import sys
from os import getenv

from aiogram import Bot, Dispatcher, Router, types
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart
from aiogram.types import Message
from aiogram.utils.markdown import hbold, hcode
import requests

TOKEN = getenv("BOT_TOKEN")

dp = Dispatcher()


@dp.message(CommandStart())
async def command_start_handler(message: Message) -> None:
    await message.answer(f"Привет, {hbold(message.from_user.full_name)}!\n"
                         f"Отправь мне СНИЛС пользователя у которого ты хочешь посмотреть статистику!")


@dp.message()
async def echo_handler(message: types.Message) -> None:
    try:
        user = message.text.strip().replace(' ', '').replace('-', '')
        data = requests.get(
            f"http://backend:80/ratings/{user}").json()
        for uni, dirs in data.items():
            await message.answer(hbold(f"{uni} ({user})") + "\n" + "\n\n".join(
                f"{hbold(direct)}:"
                f"\n\t\t\tРейтинг={hcode(info['real_rating'])}"
                f"\n\t\t\tСогласия={hcode(info['consent'])}/{hcode(info['ctrl_number'])}"
                f"\n\t\t\tЛюдей на место={hcode(info['competition'])}"
                for direct, info in dirs.items()))
    except Exception as E:
        logging.warning(str(type(E)) + ":" + str(E))


async def main():
    bot = Bot(TOKEN, parse_mode=ParseMode.HTML)
    await dp.start_polling(bot)


if __name__ == "__main__":
    logging.basicConfig(format="%(asctime)s %(levelname)s %(message)s", level=logging.INFO, stream=sys.stdout)
    asyncio.run(main())
