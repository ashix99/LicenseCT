from __future__ import annotations

import html
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, ClassVar


BASE_DIR = Path(__file__).resolve().parent
DEFAULT_ENV_PATH = BASE_DIR / ".env"


def load_env_file(path: Path) -> None:
    if not path.exists():
        return

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()

        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]

        value = (
            value.replace("\\n", "\n")
            .replace("\\r", "\r")
            .replace("\\t", "\t")
        )
        os.environ.setdefault(key, value)


@dataclass(slots=True)
class Settings:
    EN_TEXTS: ClassVar[dict[str, str]] = {
        "renew_button_text": "Activate ChatGPT Subscription",
        "support_button_text": "Contact Support",
        "confirm_button_text": "Confirm and Continue",
        "cancel_button_text": "Cancel",
        "usage_status_ready_text": "Valid and ready to use",
        "usage_status_used_text": "Already used",
        "activation_checking_message": "Checking activation code...",
        "session_checking_message": "Checking account session...",
        "welcome_message": "Please choose one of the options below.",
        "support_message": "For support, message {support}.",
        "support_hint_message": "If you could not solve the problem, message {support}.",
        "request_activation_code_message": "Please enter your activation code.\nThe activation code must not contain spaces or Persian letters.",
        "activation_info_message": "Your activation code was checked.\nApp: {app_name}\nProduct: {app_product_name}\nStatus: {usage_status}",
        "activation_invalid_message": "The activation code is invalid. Please check it and send it again.\n{support_hint}",
        "activation_used_message": "This activation code has already been used. Please send a valid activation code.\n{support_hint}",
        "activation_check_error_message": "An error occurred while checking the activation code.\n{error}\n{support_hint}",
        "request_session_message": "Please send your account session.\nTo learn how to get the account session, open the link below:\nhttps://t.me/LicenseCT/2",
        "session_invalid_message": "Please send the session completely and correctly.\nTo learn how to get the account session, open the link below:\nhttps://t.me/LicenseCT/2\n{support_hint}",
        "subscription_warning_message": "This account already has an active subscription.\nIf a new product is activated on this account, the remaining days of the current subscription will be lost.\nDo you want to continue?",
        "final_confirm_message": "Email:\n{email}\nProduct:\n{product}\nDo you want to activate it?",
        "processing_order_message": "Your order is being processed. Please wait.",
        "order_submit_error_message": "An error occurred while submitting the order.\n{error}\n{support_hint}",
        "order_poll_error_message": "An error occurred while checking the order result.\n{error}\n{support_hint}",
        "order_result_message": "Your order result:\n{result_text}",
        "order_timeout_message": "The order check timed out.\n{support_hint}",
        "cancelled_message": "The process was cancelled. Start again from the main menu.",
        "generic_error_message": "An error occurred.\n{support_hint}",
        "in_progress_message": "Your previous order is still being processed. Please wait.",
        "return_to_menu_message": "Use the main menu to place a new order.",
    }

    telegram_bot_token: str
    telegram_api_id: int
    telegram_api_hash: str
    api_base_url: str
    product_id: str
    support_username: str
    database_path: Path
    log_path: Path
    telethon_session_name: str
    session_window_seconds: int
    session_max_messages: int
    outstock_poll_seconds: int
    outstock_timeout_seconds: int
    renew_button_text: str
    support_button_text: str
    confirm_button_text: str
    cancel_button_text: str
    usage_status_ready_text: str
    usage_status_used_text: str
    activation_checking_message: str
    session_checking_message: str
    welcome_message: str
    support_message: str
    support_hint_message: str
    request_activation_code_message: str
    activation_info_message: str
    activation_invalid_message: str
    activation_used_message: str
    activation_check_error_message: str
    request_session_message: str
    session_invalid_message: str
    subscription_warning_message: str
    final_confirm_message: str
    processing_order_message: str
    order_submit_error_message: str
    order_poll_error_message: str
    order_result_message: str
    order_timeout_message: str
    cancelled_message: str
    generic_error_message: str
    in_progress_message: str
    return_to_menu_message: str

    @classmethod
    def from_env(cls) -> "Settings":
        load_env_file(DEFAULT_ENV_PATH)

        def env_str(name: str, default: str = "", *, required: bool = False) -> str:
            value = os.getenv(name, default).strip()
            if required and not value:
                raise RuntimeError(f"Environment variable '{name}' is required.")
            return value

        def env_int(name: str, default: int) -> int:
            raw = os.getenv(name)
            if raw is None or not raw.strip():
                return default
            return int(raw.strip())

        settings = cls(
            telegram_bot_token=env_str("TELEGRAM_BOT_TOKEN", required=True),
            telegram_api_id=env_int("TELEGRAM_API_ID", 0),
            telegram_api_hash=env_str("TELEGRAM_API_HASH", required=True),
            api_base_url=env_str("API_BASE_URL", "https://receipt-api.nitro.xin"),
            product_id=env_str("PRODUCT_ID", "chatgpt"),
            support_username=env_str("SUPPORT_USERNAME", "@dexashkan"),
            database_path=BASE_DIR / env_str("DATABASE_PATH", "bot_data.sqlite3"),
            log_path=BASE_DIR / env_str("LOG_PATH", "bot.log"),
            telethon_session_name=env_str("TELETHON_SESSION_NAME", "telethon_bot"),
            session_window_seconds=env_int("SESSION_WINDOW_SECONDS", 4),
            session_max_messages=env_int("SESSION_MAX_MESSAGES", 3),
            outstock_poll_seconds=env_int("OUTSTOCK_POLL_SECONDS", 10),
            outstock_timeout_seconds=env_int("OUTSTOCK_TIMEOUT_SECONDS", 600),
            renew_button_text=env_str("RENEW_BUTTON_TEXT", "فعالسازی اشتراک ChatGPT"),
            support_button_text=env_str("SUPPORT_BUTTON_TEXT", "ارتباط با پشتیبانی"),
            confirm_button_text=env_str("CONFIRM_BUTTON_TEXT", "تایید و ادامه"),
            cancel_button_text=env_str("CANCEL_BUTTON_TEXT", "انصراف"),
            usage_status_ready_text=env_str(
                "USAGE_STATUS_READY_TEXT", "سالم و قابل استفاده"
            ),
            usage_status_used_text=env_str(
                "USAGE_STATUS_USED_TEXT", "قبلا استفاده شده"
            ),
            activation_checking_message=env_str(
                "ACTIVATION_CHECKING_MESSAGE",
                "در حال بررسی کد فعالسازی...",
            ),
            session_checking_message=env_str(
                "SESSION_CHECKING_MESSAGE",
                "در حال بررسی سشن اکانت...",
            ),
            welcome_message=env_str(
                "WELCOME_MESSAGE",
                "لطفا یکی از گزینه های زیر رو انتخاب کنید",
            ),
            support_message=env_str(
                "SUPPORT_MESSAGE", "برای پشتیبانی به {support} پیام بدهید."
            ),
            support_hint_message=env_str(
                "SUPPORT_HINT_MESSAGE",
                "اگر مشکل را نتوانستید حل کنید به {support} پیام بدهید.",
            ),
            request_activation_code_message=env_str(
                "REQUEST_ACTIVATION_CODE_MESSAGE",
                "لطفا کد فعالسازی را وارد کنید.\nکد فعالسازی نباید فاصله یا حروف فارسی داشته باشد.",
            ),
            activation_info_message=env_str(
                "ACTIVATION_INFO_MESSAGE",
                "کد فعالسازی شما بررسی شد.\nنام اپ: {app_name}\nمحصول: {app_product_name}\nوضعیت: {usage_status}",
            ),
            activation_invalid_message=env_str(
                "ACTIVATION_INVALID_MESSAGE",
                "کد فعالسازی معتبر نیست. لطفا دوباره بررسی و ارسال کنید.\n{support_hint}",
            ),
            activation_used_message=env_str(
                "ACTIVATION_USED_MESSAGE",
                "این کد فعالسازی قبلا استفاده شده است. لطفا یک کد فعالسازی سالم بفرستید.\n{support_hint}",
            ),
            activation_check_error_message=env_str(
                "ACTIVATION_CHECK_ERROR_MESSAGE",
                "در بررسی کد فعالسازی خطایی رخ داد.\n{error}\n{support_hint}",
            ),
            request_session_message=env_str(
                "REQUEST_SESSION_MESSAGE",
                "لطفا سشن اکانت را بفرستید.\nبرای فهمیدن روش گرفتن سشن اکانت روی لینک زیر بزنید:\nhttps://t.me/LicenseCT/2",
            ),
            session_invalid_message=env_str(
                "SESSION_INVALID_MESSAGE",
                "لطفا سشن را کامل و به درستی وارد کنید.\nبرای آموزش گرفتن سشن اکانت روی لینک زیر بزنید:\nhttps://t.me/LicenseCT/2\n{support_hint}",
            ),
            subscription_warning_message=env_str(
                "SUBSCRIPTION_WARNING_MESSAGE",
                "این اکانت در حال حاضر اشتراک فعال دارد.\nاگر روی این اکانت محصول جدید فعال شود، روزهای باقی‌مانده اشتراک فعلی از بین می‌رود.\nآیا ادامه می‌دهید؟",
            ),
            final_confirm_message=env_str(
                "FINAL_CONFIRM_MESSAGE",
                "ایمیل:\n{email}\nمحصول:\n{product}\nفعال شود یا نه؟",
            ),
            processing_order_message=env_str(
                "PROCESSING_ORDER_MESSAGE",
                "سفارش شما در حال انجام شدن است. لطفا منتظر بمانید.",
            ),
            order_submit_error_message=env_str(
                "ORDER_SUBMIT_ERROR_MESSAGE",
                "در ثبت سفارش خطایی رخ داد.\n{error}\n{support_hint}",
            ),
            order_poll_error_message=env_str(
                "ORDER_POLL_ERROR_MESSAGE",
                "در پیگیری نتیجه سفارش خطایی رخ داد.\n{error}\n{support_hint}",
            ),
            order_result_message=env_str(
                "ORDER_RESULT_MESSAGE", "نتیجه سفارش شما:\n{result_text}"
            ),
            order_timeout_message=env_str(
                "ORDER_TIMEOUT_MESSAGE",
                "زمان بررسی سفارش به پایان رسید.\n{support_hint}",
            ),
            cancelled_message=env_str(
                "CANCELLED_MESSAGE",
                "فرآیند لغو شد. از منوی اصلی دوباره شروع کنید.",
            ),
            generic_error_message=env_str(
                "GENERIC_ERROR_MESSAGE", "خطایی رخ داد.\n{support_hint}"
            ),
            in_progress_message=env_str(
                "IN_PROGRESS_MESSAGE",
                "سفارش قبلی شما هنوز در حال انجام است. لطفا منتظر بمانید.",
            ),
            return_to_menu_message=env_str(
                "RETURN_TO_MENU_MESSAGE",
                "برای ثبت سفارش جدید از منوی اصلی استفاده کنید.",
            ),
        )

        if settings.telegram_api_id <= 0:
            raise RuntimeError("Environment variable 'TELEGRAM_API_ID' is required.")
        if settings.session_window_seconds <= 0:
            raise RuntimeError("SESSION_WINDOW_SECONDS must be greater than zero.")
        if settings.session_max_messages <= 0:
            raise RuntimeError("SESSION_MAX_MESSAGES must be greater than zero.")
        if settings.outstock_poll_seconds <= 0:
            raise RuntimeError("OUTSTOCK_POLL_SECONDS must be greater than zero.")
        if settings.outstock_timeout_seconds <= 0:
            raise RuntimeError("OUTSTOCK_TIMEOUT_SECONDS must be greater than zero.")

        return settings

    def get_text(self, field_name: str, language: str = "fa") -> str:
        if language == "en":
            return self.EN_TEXTS.get(field_name, getattr(self, field_name))
        return getattr(self, field_name)

    def render(self, template: str, *, language: str = "fa", **values: Any) -> str:
        support = html.escape(self.support_username)
        support_hint_template = self.get_text("support_hint_message", language)
        context = {
            "support": support,
            "support_hint": support_hint_template.format(support=support),
        }
        for key, value in values.items():
            context[key] = "" if value is None else html.escape(str(value))
        return template.format(**context)

    def render_key(self, field_name: str, *, language: str = "fa", **values: Any) -> str:
        return self.render(self.get_text(field_name, language), language=language, **values)
