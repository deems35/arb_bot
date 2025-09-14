# Arb Bot (Binance <-> Bybit)

Арбитражный бот: квантильный канал по ряду log(close_binance) - log(close_bybit), динамическая фильтрация пар, L1 подписка и исполнение парных рыночных ордеров.

## Установка

1. Создать виртуальное окружение:

```bash
python -m venv .venv
source .venv/bin/activate
```

2. Установить зависимости:

```bash
pip install -r requirements.txt
```

3. Заполнить API-ключи `config.yaml` либо использовать `.env`.
4. Поместить `binance-bybit.txt` в корень (пары в формате `BinanceSymbol,BybitSymbol`).

## Запуск

- Dry run (только сигналы, без реальных ордеров):

```bash
python -m src --dry-run
```

- Full run (реальные ордера; убедитесь, что ключи корректны):

```bash
python -m src
```

> ВНИМАНИЕ: Не используйте реальные ключи до тех пор, пока не протестируете в dry-run режиме и не убедитесь, что логика верна.
