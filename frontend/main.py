import asyncio
import logging
import sys
from os import getenv

from aiogram import Bot, Dispatcher, types
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart
from aiogram.types import Message
from aiogram.utils.markdown import hbold, hcode
import requests
from aiogram.client.default import DefaultBotProperties

TOKEN = "6949424987:AAHMCBHGNEP0O-Dr9z-vTDSNsSiTZ7Yl5gk"  # getenv("BOT_TOKEN")

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
            f"http://backend:80/get_real_rating/{user}"
        )
        data_json = data.json()
        if not data_json:
            await message.answer(f"Данные о '{user}' небыли найдены. Проверьте СНИЛС и/или попробуйте позднее...")
            return
        for uni, dirs in data_json.items():
            await message.answer(hbold(f"{uni} ({user})") + "\n" + "\n\n".join(
                f"{hbold(info['name'])}:"
                f"\n\t\t\tРейтинг={hcode(info['real_rating'])}"
                f"\n\t\t\tСогласия={hcode(info['consent'])}/{hcode(info['ctrl_number'])}"
                f"\n\t\t\tЛюдей на место={hcode(info['competition'])}"
                for group_id, info in dirs.items()))
    except Exception as E:
        logging.warning(str(type(E)) + ":" + str(E))
        await message.answer("Извините, произошёл внутренний сбой. Повторите попытку позднее")


async def main():
    bot = Bot(TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    await dp.start_polling(bot)


if __name__ == "__main__":
    logging.basicConfig(format="%(asctime)s %(levelname)s %(message)s", level=logging.INFO, stream=sys.stdout)
    asyncio.run(main())
