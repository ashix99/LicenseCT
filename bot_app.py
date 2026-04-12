from __future__ import annotations

import asyncio
import html
import json
import logging
import re
from datetime import datetime
from pathlib import Path
from typing import Any

try:
    from telethon import Button, TelegramClient, events
except ImportError:  # pragma: no cover - runtime dependency
    Button = None
    TelegramClient = None
    events = None

from admin_exports import (
    build_export_path,
    export_activation_history_xlsx,
    export_users_xlsx,
)
from receipt_api import ApiError, ReceiptApiClient
from session_data import (
    SessionData,
    SessionValidationError,
    combine_session_fragments,
    extract_outstock_user,
)
from settings import BASE_DIR, Settings
from storage import BotStorage, utc_now_iso

ACTIVATION_CODE_WHITESPACE_PATTERN = re.compile(r"\s")
ACTIVATION_CODE_PERSIAN_PATTERN = re.compile(r"[\u0600-\u06FF]")
CHATGPT_COMMAND_PATTERN = re.compile(r"^/chatgpt(?:@\w+)?$", re.IGNORECASE)
ADMIN_COMMAND_PATTERN = re.compile(r"^/admin(?:@\w+)?$", re.IGNORECASE)


def json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True)


class ActivationBotApp:
    UI_TEXTS = {
        "fa": {
            "choose_language_title": "انتخاب زبان",
            "choose_language_body": "لطفا زبان خود را انتخاب کنید.",
            "welcome_title": "خوش آمدید",
            "main_menu_title": "منوی اصلی",
            "order_in_progress_title": "سفارش در حال انجام است",
            "error_title": "خطا",
            "support_title": "پشتیبانی",
            "flow_cancelled_title": "فرآیند لغو شد",
            "activation_title": "فعالسازی اشتراک ChatGPT",
            "activation_invalid_title": "کد فعالسازی نامعتبر است",
            "activation_code_format_title": "کد فعالسازی نامعتبر است",
            "activation_code_format_body": "کد فعالسازی نباید فاصله یا حروف فارسی داشته باشد.\n\nلطفا کد را دوباره بدون فاصله و بدون حروف فارسی ارسال کنید.",
            "please_wait_title": "لطفا صبر کنید",
            "activation_check_error_title": "خطا در بررسی کد فعالسازی",
            "activation_used_title": "کد فعالسازی قبلا استفاده شده",
            "activation_checked_title": "کد فعالسازی بررسی شد",
            "request_session_title": "ارسال سشن اکانت",
            "session_check_error_title": "خطا در بررسی سشن اکانت",
            "session_check_unexpected": "در بررسی سشن اکانت خطای غیرمنتظره‌ای رخ داد.\nلطفا دوباره سشن اکانت را کامل و درست ارسال کنید.\n{support_hint}",
            "session_invalid_title": "سشن اکانت نامعتبر است",
            "subscription_warning_title": "هشدار اشتراک فعال",
            "final_confirm_title": "تایید نهایی سفارش",
            "processing_order_title": "سفارش در حال انجام است",
            "order_poll_error_title": "خطا در پیگیری سفارش",
            "order_timeout_title": "پاسخ سفارش دیر رسید",
            "order_submit_error_title": "خطا در ثبت سفارش",
            "unexpected_error_title": "خطای غیرمنتظره",
            "return_to_menu_title": "بازگشت به منوی اصلی",
            "language_changed_notice": "زبان تغییر کرد.",
            "request_inactive": "این درخواست دیگر فعال نیست.",
            "request_cancelled": "لغو شد.",
            "request_confirmed": "تایید شد.",
            "submitting_order_notice": "در حال ثبت سفارش...",
            "invalid_request": "درخواست نامعتبر است.",
            "language_button_text": "تغییر زبان",
            "change_account_button_text": "عوض کردن سشن اکانت",
            "retry_button_text": "تلاش دوباره",
            "change_account_notice": "لطفا سشن اکانت جدید را ارسال کنید.",
            "retry_now_hint": "لطفا دوباره تلاش کنید.",
            "unknown_value": "نامشخص",
            "success_result_title": "فعالسازی با موفقیت انجام شد",
            "failed_result_title": "فعالسازی ناموفق بود",
            "retry_later_hint": "لطفا مجدد یا بعدا تلاش کنید.",
            "result_email": "ایمیل",
            "result_product": "محصول",
            "result_app": "اپ",
            "result_activation_code": "کد فعالسازی",
            "result_activation_date": "تاریخ فعالسازی (میلادی)",
            "result_status": "وضعیت",
            "result_message": "پیام",
            "result_note": "نکته",
            "result_api_details": "جزئیات API",
            "history_button_text": "تاریخچه سفارشات",
            "user_history_title": "تاریخچه سفارشات شما",
            "user_no_orders": "هنوز سفارشی برای شما ثبت نشده است.",
            "admin_title": "پنل ادمین",
            "admin_panel_body": "ربات: {bot_label}\nکاربران ثبت‌شده: {user_count}\nفعالسازی‌های ثبت‌شده: {order_count}\nنوتیف کاربر جدید: {notify_new_user}\nنوتیف فعالسازی موفق: {notify_activation_success}\nنوتیف فعالسازی ناموفق: {notify_activation_failed}",
            "admin_unauthorized": "این بخش فقط برای ادمین ربات در دسترس است.",
            "admin_super_admin_only": "این بخش فقط برای سوپر ادمین در دسترس است.",
            "admin_history_button_text": "تاریخچه فعالسازی",
            "admin_users_button_text": "کاربرها",
            "admin_search_orders_button_text": "جستجوی فعالسازی",
            "admin_search_users_button_text": "جستجوی کاربر",
            "admin_clear_search_button_text": "پاک کردن جستجو",
            "admin_notifications_button_text": "مدیریت نوتیف‌ها",
            "admin_runtime_settings_button_text": "تنظیمات زنده",
            "admin_broadcast_button_text": "پیام همگانی",
            "admin_export_orders_button_text": "اکسل فعالسازی‌ها",
            "admin_export_users_button_text": "اکسل کاربرها",
            "admin_refresh_button_text": "بروزرسانی",
            "admin_close_button_text": "بستن پنل",
            "admin_notifications_title": "مدیریت نوتیف‌ها",
            "admin_notifications_body": "وضعیت فعلی نوتیف‌های این ربات را از دکمه‌های زیر تغییر بدهید.",
            "admin_runtime_settings_title": "تنظیمات زنده",
            "admin_runtime_settings_body": "این بخش تنظیمات runtime را بدون ری‌استارت تغییر می‌دهد.\n\nمقادیر مشترک: <code>support_username</code>، <code>guide_link</code>، <code>api_base_url</code>، <code>product_id</code>\nمقادیر عددی: <code>session_window_seconds</code>، <code>session_max_messages</code>، <code>outstock_poll_seconds</code>، <code>outstock_timeout_seconds</code>\nمتن‌ها: به‌صورت <code>field.fa=...</code> یا <code>field.en=...</code>\n\nمثال:\n<code>request_session_message.fa=لطفا سشن اکانت را به صورت فایل یا متن عادی ارسال کنید.</code>\n<code>support_username=@DexAshkan</code>\n\nنکته: token ربات و اطلاعات startup همچنان به ری‌استارت نیاز دارند.",
            "admin_runtime_edit_button_text": "ویرایش تنظیم",
            "admin_runtime_reset_button_text": "حذف override",
            "admin_runtime_prompt_title": "ویرایش تنظیم",
            "admin_runtime_prompt_body": "مقدار جدید را به‌صورت <code>key=value</code> ارسال کنید.",
            "admin_runtime_reset_prompt_title": "حذف override",
            "admin_runtime_reset_prompt_body": "کلیدی را که می‌خواهید حذف شود ارسال کنید. برای متن‌ها از <code>field.fa</code> یا <code>field.en</code> استفاده کنید.",
            "admin_runtime_saved": "تنظیم ذخیره شد.",
            "admin_runtime_reset_done": "override حذف شد.",
            "admin_runtime_invalid_key": "کلید ارسال‌شده برای ویرایش runtime معتبر نیست.",
            "admin_runtime_invalid_value": "مقدار ارسال‌شده برای این کلید معتبر نیست.",
            "admin_broadcast_title": "پیام همگانی",
            "admin_broadcast_body": "متن پیام همگانی را ارسال کنید. این پیام برای همه کاربرهای این ربات ارسال می‌شود.",
            "admin_broadcast_started": "پیام همگانی در حال ارسال است.",
            "admin_broadcast_done": "پیام همگانی ارسال شد. موفق: {sent_count} | ناموفق: {failed_count}",
            "admin_toggle_new_user": "نوتیف کاربر جدید",
            "admin_toggle_activation_success": "نوتیف فعالسازی موفق",
            "admin_toggle_activation_failed": "نوتیف فعالسازی ناموفق",
            "admin_back_button_text": "بازگشت",
            "admin_closed": "پنل ادمین بسته شد.",
            "admin_history_title": "آخرین فعالسازی‌ها",
            "admin_users_title": "آخرین کاربرها",
            "admin_history_search_prompt_title": "جستجوی فعالسازی",
            "admin_history_search_prompt_body": "ایمیل، آیدی تلگرام یا کد فعالسازی را ارسال کنید تا سفارش‌های مرتبط نمایش داده شوند.",
            "admin_users_search_prompt_title": "جستجوی کاربر",
            "admin_users_search_prompt_body": "آیدی تلگرام، نام، ایمیل یا کد فعالسازی را ارسال کنید تا کاربران مرتبط نمایش داده شوند.",
            "admin_no_orders": "هنوز فعالسازی ثبت‌شده‌ای وجود ندارد.",
            "admin_no_users": "هنوز کاربر ثبت‌شده‌ای وجود ندارد.",
            "admin_no_orders_search": "نتیجه‌ای برای این جستجو پیدا نشد.",
            "admin_no_users_search": "کاربری با این جستجو پیدا نشد.",
            "admin_export_orders_caption": "فایل اکسل فعالسازی‌های ثبت‌شده.",
            "admin_export_users_caption": "فایل اکسل کاربرهای ثبت‌شده.",
            "admin_new_user_notification_title": "کاربر جدید وارد ربات شد",
            "admin_activation_success_notification_title": "فعالسازی موفق",
            "admin_activation_failed_notification_title": "فعالسازی ناموفق",
            "admin_label_bot": "ربات",
            "admin_label_telegram_id": "آیدی تلگرام",
            "admin_label_name": "نام",
            "admin_label_username": "یوزرنیم",
            "admin_label_email": "ایمیل",
            "admin_label_product": "محصول",
            "admin_label_activation_code": "کد فعالسازی",
            "admin_label_time": "زمان",
            "admin_label_status": "وضعیت",
            "admin_label_message": "پیام",
            "admin_label_first_seen": "اولین ورود",
            "admin_label_last_seen": "آخرین ورود",
            "admin_label_total_users": "کاربران ثبت‌شده",
            "admin_label_total_orders": "فعالسازی‌های ثبت‌شده",
            "admin_status_on": "روشن",
            "admin_status_off": "خاموش",
            "admin_page_label": "صفحه {page}/{total}",
            "admin_per_page_label": "تعداد",
            "admin_sort_label": "مرتب‌سازی",
            "admin_filter_label": "فیلتر",
            "admin_search_label": "جستجو",
            "admin_orders_filter_all": "همه",
            "admin_orders_filter_success": "موفق",
            "admin_orders_filter_failed": "ناموفق",
            "admin_orders_sort_newest": "جدیدترین",
            "admin_orders_sort_oldest": "قدیمی‌ترین",
            "admin_users_filter_all": "همه",
            "admin_users_filter_with_orders": "دارای سفارش",
            "admin_users_filter_without_orders": "بدون سفارش",
            "admin_users_sort_joined_new": "عضویت جدید",
            "admin_users_sort_joined_old": "عضویت قدیم",
            "admin_users_sort_last_seen": "آخرین ورود",
            "admin_users_sort_last_order": "آخرین تراکنش",
            "admin_users_sort_name_az": "الفبا",
            "admin_users_sort_name_za": "ی-الف",
            "admin_label_total_transactions": "تعداد سفارش‌ها",
            "admin_label_last_transaction": "آخرین تراکنش",
        },
        "en": {
            "choose_language_title": "Choose Language",
            "choose_language_body": "Please choose your language.",
            "welcome_title": "Welcome",
            "main_menu_title": "Main Menu",
            "order_in_progress_title": "Order In Progress",
            "error_title": "Error",
            "support_title": "Support",
            "flow_cancelled_title": "Process Cancelled",
            "activation_title": "Activate ChatGPT Subscription",
            "activation_invalid_title": "Invalid Activation Code",
            "activation_code_format_title": "Invalid Activation Code",
            "activation_code_format_body": "The activation code must not contain spaces or Persian letters.\n\nPlease send the code again without spaces and without Persian letters.",
            "please_wait_title": "Please Wait",
            "activation_check_error_title": "Activation Code Check Error",
            "activation_used_title": "Activation Code Already Used",
            "activation_checked_title": "Activation Code Checked",
            "request_session_title": "Send Account Session",
            "session_check_error_title": "Session Check Error",
            "session_check_unexpected": "An unexpected error occurred while checking the account session.\nPlease send the account session completely and correctly again.\n{support_hint}",
            "session_invalid_title": "Invalid Account Session",
            "subscription_warning_title": "Active Subscription Warning",
            "final_confirm_title": "Final Order Confirmation",
            "processing_order_title": "Order In Progress",
            "order_poll_error_title": "Order Tracking Error",
            "order_timeout_title": "Order Response Timed Out",
            "order_submit_error_title": "Order Submission Error",
            "unexpected_error_title": "Unexpected Error",
            "return_to_menu_title": "Back To Main Menu",
            "language_changed_notice": "Language changed.",
            "request_inactive": "This request is no longer active.",
            "request_cancelled": "Cancelled.",
            "request_confirmed": "Confirmed.",
            "submitting_order_notice": "Submitting order...",
            "invalid_request": "Invalid request.",
            "language_button_text": "Change Language",
            "change_account_button_text": "Change Account Session",
            "retry_button_text": "Retry",
            "change_account_notice": "Please send a new account session.",
            "retry_now_hint": "Please try again.",
            "unknown_value": "Unknown",
            "success_result_title": "Activation Completed Successfully",
            "failed_result_title": "Activation Failed",
            "retry_later_hint": "Please try again now or later.",
            "result_email": "Email",
            "result_product": "Product",
            "result_app": "App",
            "result_activation_code": "Activation Code",
            "result_activation_date": "Activation Date (Gregorian)",
            "result_status": "Status",
            "result_message": "Message",
            "result_note": "Note",
            "result_api_details": "API Details",
            "history_button_text": "Order History",
            "user_history_title": "Your Order History",
            "user_no_orders": "No orders have been recorded for you yet.",
            "admin_title": "Admin Panel",
            "admin_panel_body": "Bot: {bot_label}\nRegistered users: {user_count}\nRecorded activations: {order_count}\nNew user notifications: {notify_new_user}\nSuccessful activation notifications: {notify_activation_success}\nFailed activation notifications: {notify_activation_failed}",
            "admin_unauthorized": "This section is only available to the bot admin.",
            "admin_super_admin_only": "This section is only available to the super admin.",
            "admin_history_button_text": "Activation History",
            "admin_users_button_text": "Users",
            "admin_search_orders_button_text": "Search Activations",
            "admin_search_users_button_text": "Search Users",
            "admin_clear_search_button_text": "Clear Search",
            "admin_notifications_button_text": "Manage Notifications",
            "admin_runtime_settings_button_text": "Runtime Settings",
            "admin_broadcast_button_text": "Broadcast",
            "admin_export_orders_button_text": "Activations Excel",
            "admin_export_users_button_text": "Users Excel",
            "admin_refresh_button_text": "Refresh",
            "admin_close_button_text": "Close Panel",
            "admin_notifications_title": "Notification Settings",
            "admin_notifications_body": "Use the buttons below to turn this bot's notifications on or off.",
            "admin_runtime_settings_title": "Runtime Settings",
            "admin_runtime_settings_body": "This section updates runtime settings without a restart.\n\nShared values: <code>support_username</code>, <code>guide_link</code>, <code>api_base_url</code>, <code>product_id</code>\nNumeric values: <code>session_window_seconds</code>, <code>session_max_messages</code>, <code>outstock_poll_seconds</code>, <code>outstock_timeout_seconds</code>\nText values: use <code>field.fa=...</code> or <code>field.en=...</code>\n\nExamples:\n<code>request_session_message.fa=Please send the session as text or file.</code>\n<code>support_username=@DexAshkan</code>\n\nNote: bot token and startup credentials still require a restart.",
            "admin_runtime_edit_button_text": "Edit Setting",
            "admin_runtime_reset_button_text": "Reset Override",
            "admin_runtime_prompt_title": "Edit Setting",
            "admin_runtime_prompt_body": "Send the new value as <code>key=value</code>.",
            "admin_runtime_reset_prompt_title": "Reset Override",
            "admin_runtime_reset_prompt_body": "Send the key to reset. For text values use <code>field.fa</code> or <code>field.en</code>.",
            "admin_runtime_saved": "The setting was saved.",
            "admin_runtime_reset_done": "The override was removed.",
            "admin_runtime_invalid_key": "The sent key is not valid for runtime editing.",
            "admin_runtime_invalid_value": "The sent value is not valid for this key.",
            "admin_broadcast_title": "Broadcast Message",
            "admin_broadcast_body": "Send the broadcast text. It will be delivered to all users of this bot.",
            "admin_broadcast_started": "The broadcast is being sent.",
            "admin_broadcast_done": "The broadcast was sent. Success: {sent_count} | Failed: {failed_count}",
            "admin_toggle_new_user": "New User Notifications",
            "admin_toggle_activation_success": "Successful Activation Notifications",
            "admin_toggle_activation_failed": "Failed Activation Notifications",
            "admin_back_button_text": "Back",
            "admin_closed": "Admin panel closed.",
            "admin_history_title": "Latest Activations",
            "admin_users_title": "Latest Users",
            "admin_history_search_prompt_title": "Search Activations",
            "admin_history_search_prompt_body": "Send an email, Telegram ID, or activation code to list matching orders.",
            "admin_users_search_prompt_title": "Search Users",
            "admin_users_search_prompt_body": "Send a Telegram ID, name, email, or activation code to list matching users.",
            "admin_no_orders": "No recorded activations yet.",
            "admin_no_users": "No registered users yet.",
            "admin_no_orders_search": "No matching activations were found for this search.",
            "admin_no_users_search": "No matching users were found for this search.",
            "admin_export_orders_caption": "Excel export for recorded activations.",
            "admin_export_users_caption": "Excel export for registered users.",
            "admin_new_user_notification_title": "New User Joined The Bot",
            "admin_activation_success_notification_title": "Activation Succeeded",
            "admin_activation_failed_notification_title": "Activation Failed",
            "admin_label_bot": "Bot",
            "admin_label_telegram_id": "Telegram ID",
            "admin_label_name": "Name",
            "admin_label_username": "Username",
            "admin_label_email": "Email",
            "admin_label_product": "Product",
            "admin_label_activation_code": "Activation Code",
            "admin_label_time": "Time",
            "admin_label_status": "Status",
            "admin_label_message": "Message",
            "admin_label_first_seen": "First Seen",
            "admin_label_last_seen": "Last Seen",
            "admin_label_total_users": "Registered Users",
            "admin_label_total_orders": "Recorded Activations",
            "admin_status_on": "On",
            "admin_status_off": "Off",
            "admin_page_label": "Page {page}/{total}",
            "admin_per_page_label": "Per Page",
            "admin_sort_label": "Sort",
            "admin_filter_label": "Filter",
            "admin_search_label": "Search",
            "admin_orders_filter_all": "All",
            "admin_orders_filter_success": "Success",
            "admin_orders_filter_failed": "Failed",
            "admin_orders_sort_newest": "Newest",
            "admin_orders_sort_oldest": "Oldest",
            "admin_users_filter_all": "All",
            "admin_users_filter_with_orders": "With Orders",
            "admin_users_filter_without_orders": "No Orders",
            "admin_users_sort_joined_new": "Newest Joined",
            "admin_users_sort_joined_old": "Oldest Joined",
            "admin_users_sort_last_seen": "Last Seen",
            "admin_users_sort_last_order": "Last Transaction",
            "admin_users_sort_name_az": "A-Z",
            "admin_users_sort_name_za": "Z-A",
            "admin_label_total_transactions": "Orders",
            "admin_label_last_transaction": "Last Transaction",
        },
    }

    RUNTIME_TEXT_FIELDS = (
        "renew_button_text",
        "support_button_text",
        "confirm_button_text",
        "cancel_button_text",
        "usage_status_ready_text",
        "usage_status_used_text",
        "activation_checking_message",
        "session_checking_message",
        "welcome_message",
        "support_message",
        "support_hint_message",
        "request_activation_code_message",
        "activation_info_message",
        "activation_invalid_message",
        "activation_used_message",
        "activation_check_error_message",
        "request_session_message",
        "session_invalid_message",
        "subscription_warning_message",
        "final_confirm_message",
        "processing_order_message",
        "order_submit_error_message",
        "order_poll_error_message",
        "order_result_message",
        "order_timeout_message",
        "cancelled_message",
        "generic_error_message",
        "in_progress_message",
        "return_to_menu_message",
    )
    RUNTIME_VALUE_FIELDS = (
        "support_username",
        "guide_link",
        "api_base_url",
        "product_id",
    )
    RUNTIME_INT_FIELDS = {
        "session_window_seconds": (1, 120),
        "session_max_messages": (1, 20),
        "outstock_poll_seconds": (1, 120),
        "outstock_timeout_seconds": (10, 3600),
    }

    def __init__(self, settings: Settings, logger: logging.Logger) -> None:
        if TelegramClient is None or events is None or Button is None:
            raise RuntimeError(
                "Telethon نصب نشده است. ابتدا وابستگی‌های requirements.txt را نصب کنید."
            )

        self.settings = settings
        self.logger = logger
        self.storage = BotStorage(settings.database_path)
        self.api = ReceiptApiClient(settings.api_base_url, settings.product_id)
        session_path = BASE_DIR / settings.telethon_session_name
        session_path.parent.mkdir(parents=True, exist_ok=True)
        self.client = TelegramClient(
            str(session_path),
            settings.telegram_api_id,
            settings.telegram_api_hash,
        )
        self.session_tasks: dict[int, asyncio.Task[Any]] = {}
        self.order_tasks: dict[int, asyncio.Task[Any]] = {}
        self.session_status_messages: dict[int, int] = {}
        self._register_handlers()

    @staticmethod
    def _mask_value(value: str | None, *, keep_start: int = 6, keep_end: int = 4) -> str:
        if not value:
            return ""
        if len(value) <= keep_start + keep_end:
            return "*" * len(value)
        return f"{value[:keep_start]}...{value[-keep_end:]}"

    def _format_panel(self, title: str, body: str, emoji: str) -> str:
        clean_body = body.strip()
        if not clean_body:
            return f"{emoji} <b>{title}</b>"
        return f"{emoji} <b>{title}</b>\n\n{clean_body}"

    def _normalize_language(self, value: str | None) -> str:
        language = str(value or "").strip().lower()
        if language in {"fa", "en"}:
            return language
        return ""

    def _language_from_state(self, state: dict[str, Any] | None) -> str:
        if not state:
            return ""
        return self._normalize_language(state.get("language"))

    def _language_for_user(self, user_id: int, state: dict[str, Any] | None = None) -> str:
        return self._language_from_state(state or self.storage.get_state(user_id)) or "fa"

    def _ui_text(self, key: str, language: str) -> str:
        normalized = self._normalize_language(language) or "fa"
        return self.UI_TEXTS[normalized][key]

    def _runtime_storage_key(self, field_name: str, language: str | None = None) -> str:
        if field_name in self.RUNTIME_TEXT_FIELDS:
            normalized = self._normalize_language(language) or "fa"
            return f"runtime.text.{field_name}.{normalized}"
        if field_name in self.RUNTIME_VALUE_FIELDS:
            return f"runtime.value.{field_name}"
        if field_name in self.RUNTIME_INT_FIELDS:
            return f"runtime.int.{field_name}"
        raise KeyError(field_name)

    def _runtime_text_value(self, field_name: str, language: str) -> str:
        normalized = self._normalize_language(language) or "fa"
        override = self.storage.get_setting(
            self._runtime_storage_key(field_name, normalized)
        )
        if override is not None and override != "":
            return override
        return self.settings.get_text(field_name, normalized)

    def _runtime_scalar_value(self, field_name: str) -> str:
        override = self.storage.get_setting(self._runtime_storage_key(field_name))
        if override is not None and override != "":
            return override
        return str(getattr(self.settings, field_name))

    def _runtime_int_value(self, field_name: str) -> int:
        override = self.storage.get_setting(self._runtime_storage_key(field_name))
        if override is None or not str(override).strip():
            return int(getattr(self.settings, field_name))
        try:
            value = int(str(override).strip())
        except ValueError:
            return int(getattr(self.settings, field_name))
        minimum, maximum = self.RUNTIME_INT_FIELDS[field_name]
        if value < minimum or value > maximum:
            return int(getattr(self.settings, field_name))
        return value

    def _render_runtime_template(
        self,
        template: str,
        *,
        language: str,
        **values: Any,
    ) -> str:
        support = html.escape(self._runtime_scalar_value("support_username"))
        guide_link = html.escape(self._runtime_scalar_value("guide_link"))
        support_hint_template = self._runtime_text_value("support_hint_message", language)
        base_context = {
            "support": support,
            "guide_link": guide_link,
        }
        support_hint = support_hint_template.format(**base_context)
        context = {
            **base_context,
            "support_hint": support_hint,
        }
        for key, value in values.items():
            context[key] = "" if value is None else html.escape(str(value))
        return template.format(**context)

    def _render_key(self, key: str, *, language: str, **values: Any) -> str:
        template = self._runtime_text_value(key, language)
        return self._render_runtime_template(template, language=language, **values)

    def _button_label(self, key: str, language: str) -> str:
        normalized = self._normalize_language(language) or "fa"
        emoji_map = {
            "renew": "✅",
            "history": "📦",
            "support": "🧑‍💻",
            "language": "🌍",
            "cancel": "❌",
            "confirm": "✅",
        }
        text_map = {
            "renew": self._runtime_text_value("renew_button_text", normalized),
            "history": self._ui_text("history_button_text", normalized),
            "support": self._runtime_text_value("support_button_text", normalized),
            "language": self._ui_text("language_button_text", normalized),
            "cancel": self._runtime_text_value("cancel_button_text", normalized),
            "confirm": self._runtime_text_value("confirm_button_text", normalized),
        }
        return f"{emoji_map[key]} {text_map[key]}"

    def _inline_label(self, key: str, language: str) -> str:
        normalized = self._normalize_language(language) or "fa"
        emoji_map = {
            "change_account": "🔄",
            "retry": "🔁",
        }
        text_map = {
            "change_account": self._ui_text("change_account_button_text", normalized),
            "retry": self._ui_text("retry_button_text", normalized),
        }
        return f"{emoji_map[key]} {text_map[key]}"

    def _button_variants(self, key: str) -> set[str]:
        return {
            self._button_label(key, "fa"),
            self._button_label(key, "en"),
        }

    def _language_selector_buttons(self) -> list[list[Any]]:
        return [[
            Button.inline("🇮🇷 فارسی", data=b"set_language:fa"),
            Button.inline("🇺🇸 English", data=b"set_language:en"),
        ]]

    def _change_account_buttons(self, language: str) -> list[list[Any]]:
        return [[
            Button.inline(
                self._inline_label("change_account", language),
                data=b"change_account",
            )
        ]]

    def _retry_order_buttons(self, language: str) -> list[list[Any]]:
        return [[
            Button.inline(
                self._inline_label("retry", language),
                data=b"retry_order",
            )
        ]]

    def _retry_activation_buttons(self, language: str) -> list[list[Any]]:
        return [[
            Button.inline(
                self._inline_label("retry", language),
                data=b"retry_activation_check",
            )
        ]]

    def _admin_panel_buttons(self, user_id: int, language: str) -> list[list[Any]]:
        buttons = [
            [
                Button.inline(
                    f"📋 {self._ui_text('admin_history_button_text', language)}",
                    data=b"admin:history",
                ),
                Button.inline(
                    f"👥 {self._ui_text('admin_users_button_text', language)}",
                    data=b"admin:users",
                ),
            ],
            [
                Button.inline(
                    f"🔎 {self._ui_text('admin_search_orders_button_text', language)}",
                    data=b"admin:search_orders",
                ),
                Button.inline(
                    f"🔍 {self._ui_text('admin_search_users_button_text', language)}",
                    data=b"admin:search_users",
                ),
            ],
            [
                Button.inline(
                    f"🔔 {self._ui_text('admin_notifications_button_text', language)}",
                    data=b"admin:notifications",
                ),
            ],
        ]
        if self._is_super_admin_user(user_id):
            buttons.append(
                [
                    Button.inline(
                        f"⚙️ {self._ui_text('admin_runtime_settings_button_text', language)}",
                        data=b"admin:runtime",
                    ),
                    Button.inline(
                        f"📣 {self._ui_text('admin_broadcast_button_text', language)}",
                        data=b"admin:broadcast",
                    ),
                ]
            )
        buttons.extend(
            [
            [
                Button.inline(
                    f"📊 {self._ui_text('admin_export_orders_button_text', language)}",
                    data=b"admin:export_orders",
                ),
                Button.inline(
                    f"📁 {self._ui_text('admin_export_users_button_text', language)}",
                    data=b"admin:export_users",
                ),
            ],
            [
                Button.inline(
                    f"🔄 {self._ui_text('admin_refresh_button_text', language)}",
                    data=b"admin:panel",
                ),
                Button.inline(
                    f"❌ {self._ui_text('admin_close_button_text', language)}",
                    data=b"admin:close",
                ),
            ],
            ]
        )
        return buttons

    def _admin_search_state_name(self, view_kind: str) -> str:
        return f"waiting_admin_{view_kind}_search"

    def _load_admin_context_payload(self, user_id: int) -> dict[str, Any]:
        state = self.storage.get_state(user_id)
        raw_payload = str(state.get("activation_payload") or "").strip()
        if not raw_payload:
            return {"admin_views": {}}
        try:
            payload = json.loads(raw_payload)
        except json.JSONDecodeError:
            return {"admin_views": {}}
        if not isinstance(payload, dict):
            return {"admin_views": {}}
        admin_views = payload.get("admin_views")
        if not isinstance(admin_views, dict):
            return {"admin_views": {}}
        return {"admin_views": admin_views}

    def _default_admin_view_context(self, view_kind: str) -> dict[str, Any]:
        if view_kind == "users":
            return {
                "page": 1,
                "per_page": 20,
                "filter_key": "all",
                "sort_key": "last_seen",
                "search_query": "",
            }
        return {
            "page": 1,
            "per_page": 20,
            "filter_key": "all",
            "sort_key": "newest",
            "search_query": "",
        }

    def _get_admin_view_context(self, user_id: int, view_kind: str) -> dict[str, Any]:
        payload = self._load_admin_context_payload(user_id)
        stored = payload["admin_views"].get(view_kind)
        if not isinstance(stored, dict):
            return self._default_admin_view_context(view_kind)
        context = self._default_admin_view_context(view_kind)
        context.update(
            {
                "page": max(1, int(stored.get("page") or context["page"])),
                "per_page": int(stored.get("per_page") or context["per_page"]),
                "filter_key": str(stored.get("filter_key") or context["filter_key"]),
                "sort_key": str(stored.get("sort_key") or context["sort_key"]),
                "search_query": str(stored.get("search_query") or ""),
            }
        )
        return context

    def _save_admin_view_context(
        self,
        user_id: int,
        view_kind: str,
        *,
        page: int,
        per_page: int,
        filter_key: str,
        sort_key: str,
        search_query: str,
        state_name: str | None = None,
    ) -> None:
        payload = self._load_admin_context_payload(user_id)
        payload["admin_views"][view_kind] = {
            "page": max(1, int(page)),
            "per_page": max(1, int(per_page)),
            "filter_key": str(filter_key),
            "sort_key": str(sort_key),
            "search_query": str(search_query or ""),
        }
        updates: dict[str, Any] = {
            "activation_payload": json.dumps(payload, ensure_ascii=False),
        }
        if state_name is not None:
            updates["state"] = state_name
        self.storage.save_state(user_id, **updates)

    def _notification_settings_buttons(self, language: str) -> list[list[Any]]:
        toggles = self.storage.get_notification_settings()
        return [
            [
                Button.inline(
                    f"{self._bool_emoji(toggles['notify_new_user'])} {self._ui_text('admin_toggle_new_user', language)}",
                    data=b"admin:toggle:notify_new_user",
                )
            ],
            [
                Button.inline(
                    f"{self._bool_emoji(toggles['notify_activation_success'])} {self._ui_text('admin_toggle_activation_success', language)}",
                    data=b"admin:toggle:notify_activation_success",
                )
            ],
            [
                Button.inline(
                    f"{self._bool_emoji(toggles['notify_activation_failed'])} {self._ui_text('admin_toggle_activation_failed', language)}",
                    data=b"admin:toggle:notify_activation_failed",
                )
            ],
            [
                Button.inline(
                    f"⬅️ {self._ui_text('admin_back_button_text', language)}",
                    data=b"admin:panel",
                )
            ],
        ]

    def _admin_view_data(
        self,
        *,
        view_kind: str,
        page: int,
        per_page: int,
        filter_key: str,
        sort_key: str,
    ) -> bytes:
        return (
            f"admin:view:{view_kind}:{page}:{per_page}:{filter_key}:{sort_key}"
        ).encode("utf-8")

    def _parse_admin_view_data(self, data: bytes) -> dict[str, Any] | None:
        try:
            decoded = data.decode("utf-8", errors="ignore")
            _, action, view_kind, page, per_page, filter_key, sort_key = decoded.split(":")
        except ValueError:
            return None
        if action != "view":
            return None
        try:
            page_value = max(1, int(page))
            per_page_value = int(per_page)
        except ValueError:
            return None
        return {
            "view_kind": view_kind,
            "page": page_value,
            "per_page": per_page_value,
            "filter_key": filter_key,
            "sort_key": sort_key,
        }

    def _user_history_view_data(
        self,
        *,
        page: int,
        per_page: int,
        filter_key: str,
        sort_key: str,
    ) -> bytes:
        return f"userhistory:view:{page}:{per_page}:{filter_key}:{sort_key}".encode("utf-8")

    def _parse_user_history_view_data(self, data: bytes) -> dict[str, Any] | None:
        try:
            decoded = data.decode("utf-8", errors="ignore")
            _, action, page, per_page, filter_key, sort_key = decoded.split(":")
        except ValueError:
            return None
        if action != "view":
            return None
        try:
            page_value = max(1, int(page))
            per_page_value = int(per_page)
        except ValueError:
            return None
        return {
            "page": page_value,
            "per_page": per_page_value,
            "filter_key": filter_key,
            "sort_key": sort_key,
        }

    def _page_count(self, total_items: int, per_page: int) -> int:
        if per_page <= 0:
            return 1
        return max(1, (total_items + per_page - 1) // per_page)

    def _page_slice(self, page: int, per_page: int) -> tuple[int, int]:
        normalized_page = max(1, page)
        normalized_per_page = max(1, per_page)
        return normalized_page, (normalized_page - 1) * normalized_per_page

    def _history_per_page_options(self) -> list[int]:
        return [20, 50, 100]

    def _users_per_page_options(self) -> list[int]:
        return [20, 50, 100]

    def _history_filter_options(self, language: str) -> list[tuple[str, str]]:
        return [
            ("all", self._ui_text("admin_orders_filter_all", language)),
            ("success", self._ui_text("admin_orders_filter_success", language)),
            ("failed", self._ui_text("admin_orders_filter_failed", language)),
        ]

    def _history_sort_options(self, language: str) -> list[tuple[str, str]]:
        return [
            ("newest", self._ui_text("admin_orders_sort_newest", language)),
            ("oldest", self._ui_text("admin_orders_sort_oldest", language)),
        ]

    def _users_filter_options(self, language: str) -> list[tuple[str, str]]:
        return [
            ("all", self._ui_text("admin_users_filter_all", language)),
            ("with_orders", self._ui_text("admin_users_filter_with_orders", language)),
            ("without_orders", self._ui_text("admin_users_filter_without_orders", language)),
        ]

    def _users_sort_options(self, language: str) -> list[tuple[str, str]]:
        return [
            ("joined_new", self._ui_text("admin_users_sort_joined_new", language)),
            ("joined_old", self._ui_text("admin_users_sort_joined_old", language)),
            ("last_seen", self._ui_text("admin_users_sort_last_seen", language)),
            ("last_order", self._ui_text("admin_users_sort_last_order", language)),
            ("name_az", self._ui_text("admin_users_sort_name_az", language)),
            ("name_za", self._ui_text("admin_users_sort_name_za", language)),
        ]

    def _selector_label(self, active: bool, text: str) -> str:
        prefix = "🟢" if active else "⚪️"
        return f"{prefix} {text}"

    def _history_view_buttons(
        self,
        *,
        language: str,
        page: int,
        total_pages: int,
        per_page: int,
        filter_key: str,
        sort_key: str,
        search_query: str,
    ) -> list[list[Any]]:
        prev_page = page - 1 if page > 1 else 1
        next_page = page + 1 if page < total_pages else total_pages
        page_label = self._ui_text("admin_page_label", language).format(
            page=page,
            total=total_pages,
        )
        buttons: list[list[Any]] = [
            [
                Button.inline(
                    "⬅️",
                    data=self._admin_view_data(
                        view_kind="history",
                        page=prev_page,
                        per_page=per_page,
                        filter_key=filter_key,
                        sort_key=sort_key,
                    ),
                ),
                Button.inline(page_label, data=b"admin:noop"),
                Button.inline(
                    "➡️",
                    data=self._admin_view_data(
                        view_kind="history",
                        page=next_page,
                        per_page=per_page,
                        filter_key=filter_key,
                        sort_key=sort_key,
                    ),
                ),
            ],
            [
                Button.inline(
                    f"🔎 {self._ui_text('admin_search_orders_button_text', language)}",
                    data=b"admin:search_orders",
                ),
                Button.inline(
                    f"🧹 {self._ui_text('admin_clear_search_button_text', language)}",
                    data=b"admin:clear_search:history",
                ),
            ]
            if search_query
            else [
                Button.inline(
                    f"🔎 {self._ui_text('admin_search_orders_button_text', language)}",
                    data=b"admin:search_orders",
                )
            ],
            [
                Button.inline(
                    self._selector_label(option == per_page, str(option)),
                    data=self._admin_view_data(
                        view_kind="history",
                        page=1,
                        per_page=option,
                        filter_key=filter_key,
                        sort_key=sort_key,
                    ),
                )
                for option in self._history_per_page_options()
            ],
            [
                Button.inline(
                    self._selector_label(option_key == filter_key, option_label),
                    data=self._admin_view_data(
                        view_kind="history",
                        page=1,
                        per_page=per_page,
                        filter_key=option_key,
                        sort_key=sort_key,
                    ),
                )
                for option_key, option_label in self._history_filter_options(language)
            ],
            [
                Button.inline(
                    self._selector_label(option_key == sort_key, option_label),
                    data=self._admin_view_data(
                        view_kind="history",
                        page=1,
                        per_page=per_page,
                        filter_key=filter_key,
                        sort_key=option_key,
                    ),
                )
                for option_key, option_label in self._history_sort_options(language)
            ],
            [
                Button.inline(
                    f"⬅️ {self._ui_text('admin_back_button_text', language)}",
                    data=b"admin:panel",
                )
            ],
        ]
        return buttons

    def _users_view_buttons(
        self,
        *,
        language: str,
        page: int,
        total_pages: int,
        per_page: int,
        filter_key: str,
        sort_key: str,
        search_query: str,
    ) -> list[list[Any]]:
        prev_page = page - 1 if page > 1 else 1
        next_page = page + 1 if page < total_pages else total_pages
        page_label = self._ui_text("admin_page_label", language).format(
            page=page,
            total=total_pages,
        )
        return [
            [
                Button.inline(
                    "⬅️",
                    data=self._admin_view_data(
                        view_kind="users",
                        page=prev_page,
                        per_page=per_page,
                        filter_key=filter_key,
                        sort_key=sort_key,
                    ),
                ),
                Button.inline(page_label, data=b"admin:noop"),
                Button.inline(
                    "➡️",
                    data=self._admin_view_data(
                        view_kind="users",
                        page=next_page,
                        per_page=per_page,
                        filter_key=filter_key,
                        sort_key=sort_key,
                    ),
                ),
            ],
            [
                Button.inline(
                    f"🔍 {self._ui_text('admin_search_users_button_text', language)}",
                    data=b"admin:search_users",
                ),
                Button.inline(
                    f"🧹 {self._ui_text('admin_clear_search_button_text', language)}",
                    data=b"admin:clear_search:users",
                ),
            ]
            if search_query
            else [
                Button.inline(
                    f"🔍 {self._ui_text('admin_search_users_button_text', language)}",
                    data=b"admin:search_users",
                )
            ],
            [
                Button.inline(
                    self._selector_label(option == per_page, str(option)),
                    data=self._admin_view_data(
                        view_kind="users",
                        page=1,
                        per_page=option,
                        filter_key=filter_key,
                        sort_key=sort_key,
                    ),
                )
                for option in self._users_per_page_options()
            ],
            [
                Button.inline(
                    self._selector_label(option_key == filter_key, option_label),
                    data=self._admin_view_data(
                        view_kind="users",
                        page=1,
                        per_page=per_page,
                        filter_key=option_key,
                        sort_key=sort_key,
                    ),
                )
                for option_key, option_label in self._users_filter_options(language)
            ],
            [
                Button.inline(
                    self._selector_label(option_key == sort_key, option_label),
                    data=self._admin_view_data(
                        view_kind="users",
                        page=1,
                        per_page=per_page,
                        filter_key=filter_key,
                        sort_key=option_key,
                    ),
                )
                for option_key, option_label in self._users_sort_options(language)[:3]
            ],
            [
                Button.inline(
                    self._selector_label(option_key == sort_key, option_label),
                    data=self._admin_view_data(
                        view_kind="users",
                        page=1,
                        per_page=per_page,
                        filter_key=filter_key,
                        sort_key=option_key,
                    ),
                )
                for option_key, option_label in self._users_sort_options(language)[3:]
            ],
            [
                Button.inline(
                    f"⬅️ {self._ui_text('admin_back_button_text', language)}",
                    data=b"admin:panel",
                )
            ],
        ]

    def _user_history_buttons(
        self,
        *,
        language: str,
        page: int,
        total_pages: int,
        per_page: int,
        filter_key: str,
        sort_key: str,
    ) -> list[list[Any]]:
        prev_page = page - 1 if page > 1 else 1
        next_page = page + 1 if page < total_pages else total_pages
        page_label = self._ui_text("admin_page_label", language).format(
            page=page,
            total=total_pages,
        )
        return [
            [
                Button.inline(
                    "⬅️",
                    data=self._user_history_view_data(
                        page=prev_page,
                        per_page=per_page,
                        filter_key=filter_key,
                        sort_key=sort_key,
                    ),
                ),
                Button.inline(page_label, data=b"admin:noop"),
                Button.inline(
                    "➡️",
                    data=self._user_history_view_data(
                        page=next_page,
                        per_page=per_page,
                        filter_key=filter_key,
                        sort_key=sort_key,
                    ),
                ),
            ],
            [
                Button.inline(
                    self._selector_label(option == per_page, str(option)),
                    data=self._user_history_view_data(
                        page=1,
                        per_page=option,
                        filter_key=filter_key,
                        sort_key=sort_key,
                    ),
                )
                for option in self._history_per_page_options()
            ],
            [
                Button.inline(
                    self._selector_label(option_key == filter_key, option_label),
                    data=self._user_history_view_data(
                        page=1,
                        per_page=per_page,
                        filter_key=option_key,
                        sort_key=sort_key,
                    ),
                )
                for option_key, option_label in self._history_filter_options(language)
            ],
            [
                Button.inline(
                    self._selector_label(option_key == sort_key, option_label),
                    data=self._user_history_view_data(
                        page=1,
                        per_page=per_page,
                        filter_key=filter_key,
                        sort_key=option_key,
                    ),
                )
                for option_key, option_label in self._history_sort_options(language)
            ],
        ]

    def _normalize_username(self, value: str | None) -> str:
        raw = str(value or "").strip().lower()
        if raw.startswith("@"):
            return raw[1:]
        return raw

    def _is_admin_command(self, value: str) -> bool:
        return bool(ADMIN_COMMAND_PATTERN.fullmatch((value or "").strip()))

    def _is_super_admin_identity(self, user_id: int, username: str | None) -> bool:
        normalized_username = self._normalize_username(username)
        super_admin_usernames = {
            self._normalize_username(item)
            for item in getattr(self.settings, "super_admin_usernames", ())
            if self._normalize_username(item)
        }
        if normalized_username and normalized_username in super_admin_usernames:
            return True
        stored_user = self.storage.get_user(user_id)
        if not stored_user:
            return False
        stored_username = self._normalize_username(stored_user.get("username"))
        return bool(stored_username and stored_username in super_admin_usernames)

    def _is_admin_identity(self, user_id: int, username: str | None) -> bool:
        normalized_username = self._normalize_username(username)
        configured_usernames = {
            self._normalize_username(item)
            for item in getattr(self.settings, "admin_usernames", ())
            if self._normalize_username(item)
        }
        super_admin_usernames = {
            self._normalize_username(item)
            for item in getattr(self.settings, "super_admin_usernames", ())
            if self._normalize_username(item)
        }
        if normalized_username and (
            normalized_username in configured_usernames
            or normalized_username in super_admin_usernames
        ):
            return True
        admin_chat_id = self.storage.get_admin_chat_id()
        return admin_chat_id == user_id if admin_chat_id is not None else False

    def _is_admin_user(self, user_id: int) -> bool:
        user = self.storage.get_user(user_id)
        if user and int(user.get("is_admin") or 0) == 1:
            return True
        return self.storage.get_admin_chat_id() == user_id

    def _is_super_admin_user(self, user_id: int) -> bool:
        user = self.storage.get_user(user_id) or {}
        return self._is_super_admin_identity(user_id, user.get("username"))

    def _api_client(self) -> ReceiptApiClient:
        return ReceiptApiClient(
            self._runtime_scalar_value("api_base_url"),
            self._runtime_scalar_value("product_id"),
        )

    def _is_retryable_api_error(self, error: ApiError) -> bool:
        if error.status_code is None:
            return True
        if int(error.status_code) >= 500:
            return True
        haystack = str(error).strip().lower()
        markers = (
            "ارتباط با api برقرار نشد",
            "service unavailable",
            "temporarily unavailable",
            "try again",
            "timeout",
            "timed out",
            "bad gateway",
            "gateway timeout",
        )
        return any(marker in haystack for marker in markers)

    def _decode_uploaded_text(self, payload: Any) -> str:
        if payload is None:
            return ""
        if isinstance(payload, str):
            candidate_path = Path(payload)
            if candidate_path.exists() and candidate_path.is_file():
                try:
                    payload = candidate_path.read_bytes()
                except OSError:
                    return payload
            else:
                return payload
        if isinstance(payload, bytearray):
            payload = bytes(payload)
        if not isinstance(payload, bytes):
            return str(payload)
        for encoding in ("utf-8-sig", "utf-8", "utf-16", "utf-16-le", "utf-16-be"):
            try:
                return payload.decode(encoding)
            except UnicodeDecodeError:
                continue
        return payload.decode("latin-1", errors="ignore")

    async def _extract_session_input(self, event: Any, fallback_text: str) -> str:
        message = getattr(event, "message", None)
        file_ref = getattr(message, "file", None)
        if file_ref is None:
            return fallback_text

        downloaded: Any = None
        if hasattr(message, "download_media"):
            downloaded = await message.download_media(file=bytes)
        elif hasattr(event, "download_media"):
            downloaded = await event.download_media(file=bytes)
        return self._decode_uploaded_text(downloaded) or fallback_text

    def _parse_runtime_setting_input(
        self,
        raw_input: str,
        *,
        language: str,
    ) -> tuple[str, str, str | None]:
        text = str(raw_input or "").strip()
        if "=" not in text:
            raise ValueError(self._ui_text("admin_runtime_invalid_value", language))
        key, value = text.split("=", 1)
        normalized_key = key.strip()
        normalized_value = value.strip()
        if not normalized_key:
            raise ValueError(self._ui_text("admin_runtime_invalid_key", language))

        if "." in normalized_key:
            base_key, suffix = normalized_key.rsplit(".", 1)
            if base_key in self.RUNTIME_TEXT_FIELDS and suffix in {"fa", "en"}:
                return base_key, normalized_value, suffix

        if normalized_key in self.RUNTIME_TEXT_FIELDS:
            return normalized_key, normalized_value, self._normalize_language(language) or "fa"
        if normalized_key in self.RUNTIME_VALUE_FIELDS or normalized_key in self.RUNTIME_INT_FIELDS:
            return normalized_key, normalized_value, None
        raise ValueError(self._ui_text("admin_runtime_invalid_key", language))

    def _save_runtime_override(
        self,
        field_name: str,
        value: str,
        *,
        language: str,
        language_suffix: str | None,
    ) -> None:
        if field_name in self.RUNTIME_INT_FIELDS:
            try:
                int_value = int(str(value).strip())
            except ValueError as exc:
                raise ValueError(self._ui_text("admin_runtime_invalid_value", language)) from exc
            minimum, maximum = self.RUNTIME_INT_FIELDS[field_name]
            if int_value < minimum or int_value > maximum:
                raise ValueError(self._ui_text("admin_runtime_invalid_value", language))
            self.storage.set_setting(
                self._runtime_storage_key(field_name),
                str(int_value),
            )
            return

        setting_key = self._runtime_storage_key(field_name, language_suffix)
        self.storage.set_setting(setting_key, str(value))

    def _clear_runtime_override(
        self,
        field_name: str,
        *,
        language_suffix: str | None,
    ) -> None:
        self.storage.set_setting(
            self._runtime_storage_key(field_name, language_suffix),
            "",
        )

    def _bot_label(self) -> str:
        return f"Bot {self.settings.bot_index}"

    def _bool_emoji(self, enabled: bool) -> str:
        return "🟢" if enabled else "⚪️"

    def _bool_label(self, enabled: bool, language: str) -> str:
        return self._ui_text("admin_status_on", language) if enabled else self._ui_text("admin_status_off", language)

    def _format_timestamp(self, value: str | None) -> str:
        raw = str(value or "").strip()
        if not raw:
            return "-"
        try:
            parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
            return parsed.astimezone().strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            return raw

    def _display_name_from_parts(
        self,
        *,
        display_name: str | None,
        first_name: str | None,
        last_name: str | None,
        username: str | None,
        user_id: int | None = None,
    ) -> str:
        cleaned_display = str(display_name or "").strip()
        if cleaned_display:
            return cleaned_display
        name = " ".join(
            part.strip()
            for part in [str(first_name or ""), str(last_name or "")]
            if part and part.strip()
        ).strip()
        if name:
            return name
        normalized_username = str(username or "").strip()
        if normalized_username:
            return f"@{normalized_username}"
        return str(user_id or self._ui_text("unknown_value", "fa"))

    def _telegram_id_text(self, username: str | None) -> str:
        normalized_username = str(username or "").strip()
        if not normalized_username:
            return "-"
        if normalized_username.startswith("@"):
            return normalized_username
        return f"@{normalized_username}"

    def _log_user_identity(self, user_id: int | None) -> str:
        bot_label = self._bot_label() if hasattr(self, "settings") else "Bot"
        if user_id is None:
            return f"{bot_label} : id:unknown"
        user = self.storage.get_user(user_id) if hasattr(self, "storage") else None
        if user:
            display_name = self._display_name_from_parts(
                display_name=user.get("display_name"),
                first_name=user.get("first_name"),
                last_name=user.get("last_name"),
                username=user.get("username"),
                user_id=user_id,
            )
            username = self._telegram_id_text(user.get("username"))
            if username != "-" and display_name and display_name != username:
                return f"{bot_label} : {display_name} [{username}]"
            if username != "-":
                return f"{bot_label} : {username}"
            if display_name and display_name != str(user_id):
                return f"{bot_label} : {display_name}"
        return f"{bot_label} : id:{user_id}"

    def _option_label(
        self,
        options: list[tuple[str, str]],
        key: str,
        fallback: str,
    ) -> str:
        for option_key, option_label in options:
            if option_key == key:
                return option_label
        return fallback

    def _admin_notification_targets(self) -> list[int]:
        targets: list[int] = []
        for row in self.storage.list_admin_users():
            user_id = int(row.get("user_id") or 0)
            if user_id > 0 and user_id not in targets:
                targets.append(user_id)
        legacy_admin_chat_id = self.storage.get_admin_chat_id()
        if legacy_admin_chat_id is not None and legacy_admin_chat_id not in targets:
            targets.append(legacy_admin_chat_id)
        return targets

    async def _register_user_activity(self, event: Any) -> None:
        user_id = getattr(event, "sender_id", None)
        if user_id is None:
            return

        sender = None
        if hasattr(event, "get_sender"):
            try:
                sender = await event.get_sender()
            except Exception:
                sender = None

        username = str(getattr(sender, "username", "") or "").strip()
        first_name = str(getattr(sender, "first_name", "") or "").strip()
        last_name = str(getattr(sender, "last_name", "") or "").strip()
        display_name = self._display_name_from_parts(
            display_name="",
            first_name=first_name,
            last_name=last_name,
            username=username,
            user_id=user_id,
        )
        is_admin = self._is_admin_identity(user_id, username)
        is_new = self.storage.upsert_user(
            user_id=user_id,
            username=username,
            first_name=first_name,
            last_name=last_name,
            display_name=display_name,
            is_admin=is_admin,
        )
        if is_admin:
            self.storage.set_admin_chat_id(user_id)

        if is_new:
            self.storage.log_event(
                user_id=user_id,
                event_type="user_registered",
                details={
                    "username": username,
                    "first_name": first_name,
                    "last_name": last_name,
                    "display_name": display_name,
                    "is_admin": is_admin,
                },
            )
            if not is_admin and self.storage.get_setting_bool("notify_new_user", True):
                await self._notify_admin_new_user(
                    user_id=user_id,
                    username=username,
                    display_name=display_name,
                )

    def _should_suggest_retry_later(self, message_text: str) -> bool:
        haystack = (message_text or "").lower()
        markers = (
            "service unavailable",
            "temporarily unavailable",
            "try again",
            "request failed",
            "timeout",
            "timed out",
            "bad gateway",
            "gateway timeout",
        )
        return any(marker in haystack for marker in markers)

    def _build_admin_panel_text(self, language: str) -> str:
        toggles = self.storage.get_notification_settings()
        return self._format_panel(
            self._ui_text("admin_title", language),
            self._render_runtime_template(
                self._ui_text("admin_panel_body", language),
                language=language,
                bot_label=self._bot_label(),
                user_count=self.storage.count_users(),
                order_count=self.storage.count_completed_orders(),
                notify_new_user=self._bool_label(toggles["notify_new_user"], language),
                notify_activation_success=self._bool_label(
                    toggles["notify_activation_success"], language
                ),
                notify_activation_failed=self._bool_label(
                    toggles["notify_activation_failed"], language
                ),
            ),
            "🛠",
        )

    def _build_orders_view(
        self,
        *,
        language: str,
        page: int,
        per_page: int,
        status_filter: str,
        sort_key: str,
        search_query: str,
    ) -> tuple[str, list[list[Any]]]:
        normalized_page, offset = self._page_slice(page, per_page)
        total_items = self.storage.count_completed_orders_filtered(
            status_filter,
            search_query=search_query,
        )
        total_pages = self._page_count(total_items, per_page)
        page_value = min(normalized_page, total_pages)
        offset = (page_value - 1) * per_page
        rows = self.storage.query_completed_orders(
            limit=per_page,
            offset=offset,
            status_filter=status_filter,
            sort_key=sort_key,
            search_query=search_query,
        )
        filter_label = self._option_label(
            self._history_filter_options(language),
            status_filter,
            self._ui_text("admin_orders_filter_all", language),
        )
        sort_label = self._option_label(
            self._history_sort_options(language),
            sort_key,
            self._ui_text("admin_orders_sort_newest", language),
        )
        summary_lines = [
            f"<b>{html.escape(self._ui_text('admin_filter_label', language))}:</b> {html.escape(filter_label)}",
            f"<b>{html.escape(self._ui_text('admin_sort_label', language))}:</b> {html.escape(sort_label)}",
        ]
        if search_query:
            summary_lines.append(
                f"<b>{html.escape(self._ui_text('admin_search_label', language))}:</b> <code>{html.escape(search_query)}</code>"
            )
        if not rows:
            return self._format_panel(
                self._ui_text("admin_history_title", language),
                "\n".join(
                    [
                        *summary_lines,
                        "",
                        self._ui_text(
                            "admin_no_orders_search" if search_query else "admin_no_orders",
                            language,
                        ),
                    ]
                ),
                "📋",
            ), self._history_view_buttons(
                language=language,
                page=1,
                total_pages=1,
                per_page=per_page,
                filter_key=status_filter,
                sort_key=sort_key,
                search_query=search_query,
            )

        chunks: list[str] = []
        for row in rows:
            emoji = "✅" if row["status"] == "success" else "❌"
            display_name = self._display_name_from_parts(
                display_name=row.get("display_name"),
                first_name=row.get("first_name"),
                last_name=row.get("last_name"),
                username=row.get("username"),
                user_id=row.get("user_id"),
            )
            telegram_id = self._telegram_id_text(row.get("username"))
            email = row.get("email") or self._ui_text("unknown_value", language)
            code = row.get("activation_code") or self._ui_text("unknown_value", language)
            product_name = row.get("product_name") or self._ui_text("unknown_value", language)
            chunks.append(
                f"{emoji} #{row['id']} | {html.escape(telegram_id)} | {html.escape(display_name)}\n"
                f"{html.escape(str(email))} | <code>{html.escape(str(code))}</code>\n"
                f"{html.escape(self._ui_text('admin_label_product', language))}: {html.escape(str(product_name))}\n"
                f"{html.escape(self._ui_text('admin_label_status', language))}: {html.escape(str(row.get('status') or '-'))} | "
                f"{html.escape(self._ui_text('admin_label_time', language))}: {html.escape(self._format_timestamp(row.get('updated_at')))}"
            )
        return (
            self._format_panel(
                self._ui_text("admin_history_title", language),
                "\n".join(summary_lines) + "\n\n" + "\n\n".join(chunks),
                "📋",
            ),
            self._history_view_buttons(
                language=language,
                page=page_value,
                total_pages=total_pages,
                per_page=per_page,
                filter_key=status_filter,
                sort_key=sort_key,
                search_query=search_query,
            ),
        )

    def _build_users_view(
        self,
        *,
        language: str,
        page: int,
        per_page: int,
        filter_key: str,
        sort_key: str,
        search_query: str,
    ) -> tuple[str, list[list[Any]]]:
        normalized_page, offset = self._page_slice(page, per_page)
        total_items = self.storage.count_users_filtered(
            filter_key,
            search_query=search_query,
        )
        total_pages = self._page_count(total_items, per_page)
        page_value = min(normalized_page, total_pages)
        offset = (page_value - 1) * per_page
        rows = self.storage.query_users(
            limit=per_page,
            offset=offset,
            filter_mode=filter_key,
            sort_key=sort_key,
            search_query=search_query,
        )
        filter_label = self._option_label(
            self._users_filter_options(language),
            filter_key,
            self._ui_text("admin_users_filter_all", language),
        )
        sort_label = self._option_label(
            self._users_sort_options(language),
            sort_key,
            self._ui_text("admin_users_sort_last_seen", language),
        )
        summary_lines = [
            f"<b>{html.escape(self._ui_text('admin_filter_label', language))}:</b> {html.escape(filter_label)}",
            f"<b>{html.escape(self._ui_text('admin_sort_label', language))}:</b> {html.escape(sort_label)}",
        ]
        if search_query:
            summary_lines.append(
                f"<b>{html.escape(self._ui_text('admin_search_label', language))}:</b> <code>{html.escape(search_query)}</code>"
            )
        if not rows:
            return self._format_panel(
                self._ui_text("admin_users_title", language),
                "\n".join(
                    [
                        *summary_lines,
                        "",
                        self._ui_text(
                            "admin_no_users_search" if search_query else "admin_no_users",
                            language,
                        ),
                    ]
                ),
                "👥",
            ), self._users_view_buttons(
                language=language,
                page=1,
                total_pages=1,
                per_page=per_page,
                filter_key=filter_key,
                sort_key=sort_key,
                search_query=search_query,
            )

        chunks: list[str] = []
        for row in rows:
            display_name = self._display_name_from_parts(
                display_name=row.get("display_name"),
                first_name=row.get("first_name"),
                last_name=row.get("last_name"),
                username=row.get("username"),
                user_id=row.get("user_id"),
            )
            username = self._telegram_id_text(row.get("username"))
            total_orders = int(row.get("total_orders") or 0)
            last_order_at = self._format_timestamp(row.get("last_order_at"))
            chunks.append(
                f"👤 {html.escape(display_name)}\n"
                f"{html.escape(self._ui_text('admin_label_telegram_id', language))}: {html.escape(username)} | {html.escape(self._ui_text('admin_label_first_seen', language))}: {html.escape(self._format_timestamp(row.get('first_seen_at')))}\n"
                f"{html.escape(self._ui_text('admin_label_last_seen', language))}: {html.escape(self._format_timestamp(row.get('last_seen_at')))} | "
                f"{html.escape(self._ui_text('admin_label_total_transactions', language))}: {total_orders} | "
                f"{html.escape(self._ui_text('admin_label_last_transaction', language))}: {html.escape(last_order_at)}"
            )
        return (
            self._format_panel(
                self._ui_text("admin_users_title", language),
                "\n".join(summary_lines) + "\n\n" + "\n\n".join(chunks),
                "👥",
            ),
            self._users_view_buttons(
                language=language,
                page=page_value,
                total_pages=total_pages,
                per_page=per_page,
                filter_key=filter_key,
                sort_key=sort_key,
                search_query=search_query,
            ),
        )

    def _build_user_history_view(
        self,
        *,
        user_id: int,
        language: str,
        page: int,
        per_page: int,
        status_filter: str,
        sort_key: str,
    ) -> tuple[str, list[list[Any]]]:
        normalized_page, _ = self._page_slice(page, per_page)
        total_items = self.storage.count_user_completed_orders_filtered(
            user_id=user_id,
            status_filter=status_filter,
        )
        total_pages = self._page_count(total_items, per_page)
        page_value = min(normalized_page, total_pages)
        offset = (page_value - 1) * per_page
        rows = self.storage.query_user_completed_orders(
            user_id=user_id,
            limit=per_page,
            offset=offset,
            status_filter=status_filter,
            sort_key=sort_key,
        )
        filter_label = self._option_label(
            self._history_filter_options(language),
            status_filter,
            self._ui_text("admin_orders_filter_all", language),
        )
        sort_label = self._option_label(
            self._history_sort_options(language),
            sort_key,
            self._ui_text("admin_orders_sort_newest", language),
        )
        summary_lines = [
            f"<b>{html.escape(self._ui_text('admin_filter_label', language))}:</b> {html.escape(filter_label)}",
            f"<b>{html.escape(self._ui_text('admin_sort_label', language))}:</b> {html.escape(sort_label)}",
        ]
        if not rows:
            return (
                self._format_panel(
                    self._ui_text("user_history_title", language),
                    "\n".join([*summary_lines, "", self._ui_text("user_no_orders", language)]),
                    "📦",
                ),
                self._user_history_buttons(
                    language=language,
                    page=1,
                    total_pages=1,
                    per_page=per_page,
                    filter_key=status_filter,
                    sort_key=sort_key,
                ),
            )

        chunks: list[str] = []
        unknown_value = self._ui_text("unknown_value", language)
        for row in rows:
            emoji = "✅" if row["status"] == "success" else "❌"
            chunks.append(
                "\n".join(
                    [
                        f"{emoji} #{row['id']}",
                        f"<b>{html.escape(self._ui_text('admin_label_product', language))}:</b> {html.escape(str(row.get('product_name') or unknown_value))}",
                        f"<b>{html.escape(self._ui_text('result_email', language))}:</b> {html.escape(str(row.get('email') or unknown_value))}",
                        f"<b>{html.escape(self._ui_text('result_activation_code', language))}:</b> <code>{html.escape(str(row.get('activation_code') or unknown_value))}</code>",
                        f"<b>{html.escape(self._ui_text('result_activation_date', language))}:</b> {html.escape(self._format_timestamp(row.get('updated_at')))}",
                        f"<b>{html.escape(self._ui_text('result_status', language))}:</b> {html.escape(str(row.get('status') or unknown_value))}",
                    ]
                )
            )
        return (
            self._format_panel(
                self._ui_text("user_history_title", language),
                "\n".join(summary_lines) + "\n\n" + "\n\n".join(chunks),
                "📦",
            ),
            self._user_history_buttons(
                language=language,
                page=page_value,
                total_pages=total_pages,
                per_page=per_page,
                filter_key=status_filter,
                sort_key=sort_key,
            ),
        )

    def _build_notification_settings_text(self, language: str) -> str:
        return self._format_panel(
            self._ui_text("admin_notifications_title", language),
            self._ui_text("admin_notifications_body", language),
            "🔔",
        )

    async def _notify_admin_new_user(
        self,
        *,
        user_id: int,
        username: str,
        display_name: str,
    ) -> None:
        admin_chat_ids = self._admin_notification_targets()
        if not admin_chat_ids:
            return

        username_text = self._telegram_id_text(username)
        for admin_chat_id in admin_chat_ids:
            admin_language = self._language_for_user(admin_chat_id)
            body = "\n".join(
                [
                    f"<b>{html.escape(self._ui_text('admin_label_bot', admin_language))}:</b> {html.escape(self._bot_label())}",
                    f"<b>{html.escape(self._ui_text('admin_label_telegram_id', admin_language))}:</b> {html.escape(username_text)}",
                    f"<b>{html.escape(self._ui_text('admin_label_name', admin_language))}:</b> {html.escape(display_name)}",
                    f"<b>{html.escape(self._ui_text('admin_label_time', admin_language))}:</b> {html.escape(self._format_timestamp(utc_now_iso()))}",
                ]
            )
            await self._send_message(
                admin_chat_id,
                self._format_panel(
                    self._ui_text("admin_new_user_notification_title", admin_language),
                    body,
                    "🆕",
                ),
                sticker_kind="info",
            )

    async def _notify_admin_activation_result(
        self,
        *,
        order_id: int,
        success: bool,
        message_text: str = "",
    ) -> None:
        setting_key = "notify_activation_success" if success else "notify_activation_failed"
        if not self.storage.get_setting_bool(setting_key, True):
            return

        admin_chat_ids = self._admin_notification_targets()
        if not admin_chat_ids:
            return

        order = self.storage.get_order(order_id)
        if not order:
            return
        user = self.storage.get_user(int(order["user_id"])) or {}
        title_key = (
            "admin_activation_success_notification_title"
            if success
            else "admin_activation_failed_notification_title"
        )
        title_emoji = "✅" if success else "❌"
        display_name = self._display_name_from_parts(
            display_name=user.get("display_name"),
            first_name=user.get("first_name"),
            last_name=user.get("last_name"),
            username=user.get("username"),
            user_id=order.get("user_id"),
        )
        username_text = self._telegram_id_text(user.get("username"))
        effective_message = self._sanitize_api_text(message_text or str(order.get("task_result") or "")).strip()
        for admin_chat_id in admin_chat_ids:
            admin_language = self._language_for_user(admin_chat_id)
            lines = [
                f"<b>{html.escape(self._ui_text('admin_label_bot', admin_language))}:</b> {html.escape(self._bot_label())}",
                f"<b>{html.escape(self._ui_text('admin_label_telegram_id', admin_language))}:</b> {html.escape(username_text)}",
                f"<b>{html.escape(self._ui_text('admin_label_name', admin_language))}:</b> {html.escape(display_name)}",
                f"<b>{html.escape(self._ui_text('admin_label_email', admin_language))}:</b> {html.escape(str(order.get('email') or '-'))}",
                f"<b>{html.escape(self._ui_text('admin_label_product', admin_language))}:</b> {html.escape(str(order.get('product_name') or '-'))}",
                f"<b>{html.escape(self._ui_text('admin_label_activation_code', admin_language))}:</b> <code>{html.escape(str(order.get('activation_code') or '-'))}</code>",
                f"<b>{html.escape(self._ui_text('admin_label_time', admin_language))}:</b> {html.escape(self._format_timestamp(order.get('updated_at')))}",
                f"<b>{html.escape(self._ui_text('admin_label_status', admin_language))}:</b> {html.escape(str(order.get('status') or '-'))}",
            ]
            if effective_message:
                lines.append(
                    f"<b>{html.escape(self._ui_text('admin_label_message', admin_language))}:</b> {html.escape(effective_message[:1000])}"
                )
            await self._send_message(
                admin_chat_id,
                self._format_panel(
                    self._ui_text(title_key, admin_language),
                    "\n".join(lines),
                    title_emoji,
                ),
                sticker_kind="info" if success else "warning",
            )

    async def send_admin_panel(
        self,
        user_id: int,
        *,
        responder: Any | None = None,
    ) -> None:
        language = self._language_for_user(user_id)
        text = self._build_admin_panel_text(language)
        if responder is None:
            await self._send_message(
                user_id,
                text,
                buttons=self._admin_panel_buttons(user_id, language),
                sticker_kind="info",
            )
            return
        await self._reply(
            user_id,
            responder,
            text,
            buttons=self._admin_panel_buttons(user_id, language),
            sticker_kind="info",
        )

    async def send_notification_settings_panel(self, user_id: int) -> None:
        language = self._language_for_user(user_id)
        await self._send_message(
            user_id,
            self._build_notification_settings_text(language),
            buttons=self._notification_settings_buttons(language),
            sticker_kind="info",
        )

    async def send_admin_search_prompt(self, user_id: int, view_kind: str) -> None:
        language = self._language_for_user(user_id)
        context = self._get_admin_view_context(user_id, view_kind)
        self._save_admin_view_context(
            user_id,
            view_kind,
            page=context["page"],
            per_page=context["per_page"],
            filter_key=context["filter_key"],
            sort_key=context["sort_key"],
            search_query=context["search_query"],
            state_name=self._admin_search_state_name(view_kind),
        )
        title_key = (
            "admin_history_search_prompt_title"
            if view_kind == "history"
            else "admin_users_search_prompt_title"
        )
        body_key = (
            "admin_history_search_prompt_body"
            if view_kind == "history"
            else "admin_users_search_prompt_body"
        )
        await self._send_message(
            user_id,
            self._format_panel(
                self._ui_text(title_key, language),
                self._ui_text(body_key, language),
                "🔎" if view_kind == "history" else "🔍",
            ),
            buttons=[[
                Button.inline(
                    f"⬅️ {self._ui_text('admin_back_button_text', language)}",
                    data=b"admin:history" if view_kind == "history" else b"admin:users",
                )
            ]],
            sticker_kind="info",
        )

    async def _show_admin_orders_view(
        self,
        event: Any,
        *,
        user_id: int,
        language: str,
        page: int,
        per_page: int,
        status_filter: str,
        sort_key: str,
        search_query: str,
    ) -> None:
        self._save_admin_view_context(
            user_id,
            "history",
            page=page,
            per_page=per_page,
            filter_key=status_filter,
            sort_key=sort_key,
            search_query=search_query,
            state_name="idle",
        )
        text, buttons = self._build_orders_view(
            language=language,
            page=page,
            per_page=per_page,
            status_filter=status_filter,
            sort_key=sort_key,
            search_query=search_query,
        )
        await self._edit_admin_message(
            event,
            text,
            buttons=buttons,
        )

    async def _show_admin_users_view(
        self,
        event: Any,
        *,
        user_id: int,
        language: str,
        page: int,
        per_page: int,
        filter_key: str,
        sort_key: str,
        search_query: str,
    ) -> None:
        self._save_admin_view_context(
            user_id,
            "users",
            page=page,
            per_page=per_page,
            filter_key=filter_key,
            sort_key=sort_key,
            search_query=search_query,
            state_name="idle",
        )
        text, buttons = self._build_users_view(
            language=language,
            page=page,
            per_page=per_page,
            filter_key=filter_key,
            sort_key=sort_key,
            search_query=search_query,
        )
        await self._edit_admin_message(
            event,
            text,
            buttons=buttons,
        )

    async def _reply_with_admin_view(
        self,
        user_id: int,
        responder: Any,
        *,
        view_kind: str,
        language: str,
        page: int,
        per_page: int,
        filter_key: str,
        sort_key: str,
        search_query: str,
    ) -> None:
        self._save_admin_view_context(
            user_id,
            view_kind,
            page=page,
            per_page=per_page,
            filter_key=filter_key,
            sort_key=sort_key,
            search_query=search_query,
            state_name="idle",
        )
        if view_kind == "history":
            text, buttons = self._build_orders_view(
                language=language,
                page=page,
                per_page=per_page,
                status_filter=filter_key,
                sort_key=sort_key,
                search_query=search_query,
            )
        else:
            text, buttons = self._build_users_view(
                language=language,
                page=page,
                per_page=per_page,
                filter_key=filter_key,
                sort_key=sort_key,
                search_query=search_query,
            )
        await self._reply(
            user_id,
            responder,
            text,
            buttons=buttons,
            sticker_kind="info",
        )

    async def send_user_history(
        self,
        user_id: int,
        *,
        responder: Any | None = None,
        page: int = 1,
        per_page: int = 20,
        status_filter: str = "all",
        sort_key: str = "newest",
    ) -> None:
        language = self._language_for_user(user_id)
        text, buttons = self._build_user_history_view(
            user_id=user_id,
            language=language,
            page=page,
            per_page=per_page,
            status_filter=status_filter,
            sort_key=sort_key,
        )
        if responder is None:
            await self._send_message(
                user_id,
                text,
                buttons=buttons,
                sticker_kind="info",
            )
            return
        await self._reply(
            user_id,
            responder,
            text,
            buttons=buttons,
            sticker_kind="info",
        )

    async def send_orders_export(self, user_id: int) -> None:
        language = self._language_for_user(user_id)
        rows = self.storage.list_all_completed_orders()
        export_rows = [
            [
                row["id"],
                row["status"],
                row["user_id"],
                self._display_name_from_parts(
                    display_name=row.get("display_name"),
                    first_name=row.get("first_name"),
                    last_name=row.get("last_name"),
                    username=row.get("username"),
                    user_id=row.get("user_id"),
                ),
                f"@{row['username']}" if row.get("username") else "",
                row.get("email") or "",
                row.get("activation_code") or "",
                row.get("app_name") or "",
                row.get("product_name") or "",
                self._format_timestamp(row.get("created_at")),
                self._format_timestamp(row.get("updated_at")),
                row.get("task_id") or "",
            ]
            for row in rows
        ]
        export_path = build_export_path(
            self.settings.exports_path,
            f"activations_bot{self.settings.bot_index}",
        )
        export_activation_history_xlsx(export_path, export_rows)
        await self.client.send_file(
            user_id,
            str(export_path),
            caption=self._ui_text("admin_export_orders_caption", language),
        )

    async def send_users_export(self, user_id: int) -> None:
        language = self._language_for_user(user_id)
        rows = self.storage.list_all_users()
        export_rows = [
            [
                row["user_id"],
                self._display_name_from_parts(
                    display_name=row.get("display_name"),
                    first_name=row.get("first_name"),
                    last_name=row.get("last_name"),
                    username=row.get("username"),
                    user_id=row.get("user_id"),
                ),
                f"@{row['username']}" if row.get("username") else "",
                "Yes" if int(row.get("is_admin") or 0) else "No",
                self._format_timestamp(row.get("first_seen_at")),
                self._format_timestamp(row.get("last_seen_at")),
            ]
            for row in rows
        ]
        export_path = build_export_path(
            self.settings.exports_path,
            f"users_bot{self.settings.bot_index}",
        )
        export_users_xlsx(export_path, export_rows)
        await self.client.send_file(
            user_id,
            str(export_path),
            caption=self._ui_text("admin_export_users_caption", language),
        )

    async def _send_optional_sticker(self, user_id: int, kind: str | None) -> None:
        return

    async def _send_message(
        self,
        user_id: int,
        text: str,
        *,
        buttons: Any | None = None,
        link_preview: bool = True,
        sticker_kind: str | None = None,
    ) -> Any:
        await self._send_optional_sticker(user_id, sticker_kind)
        return await self.client.send_message(
            user_id,
            text,
            buttons=buttons,
            link_preview=link_preview,
            parse_mode="html",
        )

    async def _reply(
        self,
        user_id: int,
        responder: Any,
        text: str,
        *,
        buttons: Any | None = None,
        link_preview: bool = True,
        sticker_kind: str | None = None,
    ) -> Any:
        await self._send_optional_sticker(user_id, sticker_kind)
        return await responder(
            text,
            buttons=buttons,
            link_preview=link_preview,
            parse_mode="html",
        )

    async def _edit_admin_message(
        self,
        event: Any,
        text: str,
        *,
        buttons: Any | None = None,
        link_preview: bool = True,
    ) -> None:
        try:
            await event.edit(
                text,
                buttons=buttons,
                link_preview=link_preview,
                parse_mode="html",
            )
        except Exception:
            await self._send_message(
                event.sender_id,
                text,
                buttons=buttons,
                link_preview=link_preview,
                sticker_kind="info",
            )

    async def _delete_message(self, user_id: int, message: Any | None) -> None:
        if message is None:
            return

        message_id = getattr(message, "id", None)
        if message_id is None and isinstance(message, int):
            message_id = message
        if message_id is None:
            return

        try:
            await self.client.delete_messages(user_id, [message_id])
        except Exception:  # pragma: no cover - runtime fallback
            return

    async def _clear_session_status_message(self, user_id: int) -> None:
        message_id = self.session_status_messages.pop(user_id, None)
        await self._delete_message(user_id, message_id)

    def _register_handlers(self) -> None:
        @self.client.on(events.NewMessage(pattern=r"^/start$"))
        async def start_handler(event: Any) -> None:
            await self._safely_handle(event, self.handle_start)

        @self.client.on(events.NewMessage(incoming=True))
        async def message_handler(event: Any) -> None:
            await self._safely_handle(event, self.handle_message)

        @self.client.on(events.CallbackQuery())
        async def callback_handler(event: Any) -> None:
            await self._safely_handle(event, self.handle_callback)

    async def _safely_handle(self, event: Any, handler: Any) -> None:
        if not getattr(event, "is_private", False):
            return
        try:
            await self._register_user_activity(event)
            await handler(event)
        except Exception as exc:  # pragma: no cover - runtime fallback
            user_id = getattr(event, "sender_id", None)
            self.logger.exception(
                "Unhandled bot error for user=%s",
                self._log_user_identity(user_id),
            )
            self.storage.log_event(
                user_id=user_id,
                event_type="unhandled_exception",
                details={"error": str(exc)},
            )
            if user_id is not None:
                language = self._language_for_user(user_id)
                await self._send_message(
                    user_id,
                    self._format_panel(
                        self._ui_text("error_title", language),
                        self._render_key("generic_error_message", language=language),
                        "⚠️",
                    ),
                    sticker_kind="warning",
                )

    def main_menu_buttons(self, language: str) -> list[list[Any]]:
        return [
            [Button.text(self._button_label("renew", language), resize=True)],
            [Button.text(self._button_label("history", language), resize=True)],
            [Button.text(self._button_label("support", language), resize=True)],
            [Button.text(self._button_label("language", language), resize=True)],
        ]

    def flow_menu_buttons(self, language: str) -> list[list[Any]]:
        return [
            [Button.text(self._button_label("cancel", language), resize=True)],
            [Button.text(self._button_label("support", language), resize=True)],
        ]

    def menu_buttons_for_state(self, state_name: str, language: str) -> list[list[Any]]:
        if state_name and state_name != "idle":
            return self.flow_menu_buttons(language)
        return self.main_menu_buttons(language)

    def confirm_buttons(self, confirm_data: bytes, language: str) -> list[list[Any]]:
        return [
            [
                Button.inline(self._button_label("confirm", language), data=confirm_data),
                Button.inline(self._button_label("cancel", language), data=b"cancel_flow"),
            ]
        ]

    async def _send_language_selector(
        self,
        user_id: int,
        *,
        responder: Any | None = None,
        language: str = "fa",
    ) -> None:
        text = self._format_panel(
            self._ui_text("choose_language_title", language),
            self._ui_text("choose_language_body", language),
            "🌍",
        )
        if responder is None:
            await self._send_message(
                user_id,
                text,
                buttons=self._language_selector_buttons(),
            )
            return
        await self._reply(
            user_id,
            responder,
            text,
            buttons=self._language_selector_buttons(),
        )

    async def _send_main_menu(
        self,
        user_id: int,
        *,
        responder: Any | None = None,
        title_key: str = "welcome_title",
    ) -> None:
        language = self._language_for_user(user_id)
        text = self._format_panel(
            self._ui_text(title_key, language),
            self._render_key("welcome_message", language=language),
            "✨",
        )
        if responder is None:
            await self._send_message(
                user_id,
                text,
                buttons=self.main_menu_buttons(language),
            )
            return
        await self._reply(
            user_id,
            responder,
            text,
            buttons=self.main_menu_buttons(language),
        )

    def _normalize_activation_code(self, value: str) -> str:
        return value or ""

    def _is_valid_activation_code(self, value: str) -> bool:
        raw_value = value or ""
        if ACTIVATION_CODE_WHITESPACE_PATTERN.search(raw_value):
            return False
        if ACTIVATION_CODE_PERSIAN_PATTERN.search(raw_value):
            return False
        return True

    def _is_chatgpt_command(self, value: str) -> bool:
        return bool(CHATGPT_COMMAND_PATTERN.fullmatch((value or "").strip()))

    def _sanitize_api_text(self, value: str) -> str:
        return re.sub(r"(?i)\bcdk\b", "activation code", value or "")

    def _resolve_activation_code(self, submitted_code: str, payload: dict[str, Any]) -> str:
        return str(payload.get("code") or submitted_code).strip() or submitted_code

    def _sanitize_payload_for_user(self, value: Any) -> Any:
        if isinstance(value, dict):
            return {
                self._sanitize_api_text(str(key)): self._sanitize_payload_for_user(item)
                for key, item in value.items()
            }
        if isinstance(value, list):
            return [self._sanitize_payload_for_user(item) for item in value]
        if isinstance(value, str):
            return self._sanitize_api_text(value)
        return value

    def _build_activation_code_format_message(self, language: str) -> str:
        return self._format_panel(
            self._ui_text("activation_code_format_title", language),
            self._ui_text("activation_code_format_body", language),
            "⚠️",
        )

    def _build_order_result_panel(
        self,
        *,
        language: str,
        activation_code: str,
        app_name: str,
        product_name: str,
        email: str,
        result: dict[str, Any],
    ) -> str:
        success = bool(result.get("success"))
        title = (
            self._ui_text("success_result_title", language)
            if success
            else self._ui_text("failed_result_title", language)
        )
        emoji = "✅" if success else "❌"
        activated_at = datetime.now().astimezone().strftime("%Y-%m-%d %H:%M:%S")
        status_text = str(result.get("status") or "unknown")
        message_text = self._sanitize_api_text(
            str(result.get("message") or result.get("error") or "")
        ).strip()
        unknown_value = self._ui_text("unknown_value", language)

        lines = [
            f"<b>{html.escape(self._ui_text('result_email', language))}:</b> {html.escape(email or unknown_value)}",
            f"<b>{html.escape(self._ui_text('result_product', language))}:</b> {html.escape(product_name or unknown_value)}",
            f"<b>{html.escape(self._ui_text('result_app', language))}:</b> {html.escape(app_name or unknown_value)}",
            f"<b>{html.escape(self._ui_text('result_activation_code', language))}:</b> <code>{html.escape(activation_code)}</code>",
            f"<b>{html.escape(self._ui_text('result_activation_date', language))}:</b> {html.escape(activated_at)}",
            f"<b>{html.escape(self._ui_text('result_status', language))}:</b> {html.escape(status_text)}",
        ]

        if message_text:
            lines.append(
                f"<b>{html.escape(self._ui_text('result_message', language))}:</b> {html.escape(message_text)}"
            )
            if not success and self._should_suggest_retry_later(message_text):
                lines.append(
                    f"<b>{html.escape(self._ui_text('result_note', language))}:</b> {html.escape(self._ui_text('retry_later_hint', language))}"
                )
        elif not success:
            lines.append(f"<b>{html.escape(self._ui_text('result_api_details', language))}:</b>")
            lines.append(
                f"<pre>{html.escape(json_dumps(self._sanitize_payload_for_user(result)))}</pre>"
            )

        return f"{emoji} <b>{title}</b>\n\n" + "\n".join(lines)

    async def run(self) -> None:
        await self.client.start(bot_token=self.settings.telegram_bot_token)
        self.logger.info("Bot started")
        await self.client.run_until_disconnected()

    async def handle_start(self, event: Any) -> None:
        user_id = event.sender_id
        state = self.storage.get_state(user_id)
        language = self._language_from_state(state)

        if not language:
            await self.reset_user_flow(user_id, force=True)
            self.storage.log_event(
                user_id=user_id,
                event_type="start_language_selection",
                details="language_required",
            )
            self.logger.info(
                "start_language_selection user=%s",
                self._log_user_identity(user_id),
            )
            await self._send_language_selector(user_id, responder=event.respond, language="fa")
            return

        if state["state"] == "processing_order":
            await self._reply(
                user_id,
                event.respond,
                self._format_panel(
                    self._ui_text("order_in_progress_title", language),
                    self._render_key("in_progress_message", language=language),
                    "⏳",
                ),
                buttons=self.flow_menu_buttons(language),
                sticker_kind="processing",
            )
            return

        await self.reset_user_flow(user_id, force=True)
        self.storage.log_event(user_id=user_id, event_type="start", details="start")
        self.logger.info("start user=%s", self._log_user_identity(user_id))
        await self._send_main_menu(user_id, responder=event.respond)

    async def handle_admin_search_input(self, event: Any, view_kind: str) -> None:
        user_id = event.sender_id
        state = self.storage.get_state(user_id)
        language = self._language_from_state(state) or "fa"
        search_query = (event.raw_text or "").strip()
        context = self._get_admin_view_context(user_id, view_kind)

        self.storage.log_event(
            user_id=user_id,
            event_type="admin_search_submitted",
            details={
                "view_kind": view_kind,
                "search_query": search_query,
                "filter_key": context["filter_key"],
                "sort_key": context["sort_key"],
                "per_page": context["per_page"],
            },
        )

        await self._reply_with_admin_view(
            user_id,
            event.respond,
            view_kind=view_kind,
            language=language,
            page=1,
            per_page=context["per_page"],
            filter_key=context["filter_key"],
            sort_key=context["sort_key"],
            search_query=search_query,
        )

    async def handle_runtime_setting_input(self, event: Any) -> None:
        user_id = event.sender_id
        language = self._language_for_user(user_id)
        try:
            field_name, value, language_suffix = self._parse_runtime_setting_input(
                event.raw_text or "",
                language=language,
            )
            self._save_runtime_override(
                field_name,
                value,
                language=language,
                language_suffix=language_suffix,
            )
        except ValueError as exc:
            await self._reply(
                user_id,
                event.respond,
                self._format_panel(
                    self._ui_text("error_title", language),
                    str(exc),
                    "⚠️",
                ),
                sticker_kind="warning",
            )
            return

        self.storage.save_state(user_id, state="idle")
        self.storage.log_event(
            user_id=user_id,
            event_type="runtime_setting_saved",
            details={"input": event.raw_text or ""},
        )
        await self._reply(
            user_id,
            event.respond,
            self._format_panel(
                self._ui_text("admin_runtime_settings_title", language),
                self._ui_text("admin_runtime_saved", language),
                "✅",
            ),
            buttons=self._admin_panel_buttons(user_id, language),
            sticker_kind="info",
        )

    async def handle_runtime_setting_reset_input(self, event: Any) -> None:
        user_id = event.sender_id
        language = self._language_for_user(user_id)
        raw_key = (event.raw_text or "").strip()
        language_suffix: str | None = None
        field_name = raw_key
        if "." in raw_key:
            base_key, suffix = raw_key.rsplit(".", 1)
            if base_key in self.RUNTIME_TEXT_FIELDS and suffix in {"fa", "en"}:
                field_name = base_key
                language_suffix = suffix
        elif field_name in self.RUNTIME_TEXT_FIELDS:
            language_suffix = language

        if (
            field_name not in self.RUNTIME_TEXT_FIELDS
            and field_name not in self.RUNTIME_VALUE_FIELDS
            and field_name not in self.RUNTIME_INT_FIELDS
        ):
            await self._reply(
                user_id,
                event.respond,
                self._format_panel(
                    self._ui_text("error_title", language),
                    self._ui_text("admin_runtime_invalid_key", language),
                    "⚠️",
                ),
                sticker_kind="warning",
            )
            return

        self._clear_runtime_override(field_name, language_suffix=language_suffix)
        self.storage.save_state(user_id, state="idle")
        self.storage.log_event(
            user_id=user_id,
            event_type="runtime_setting_reset",
            details={"key": raw_key},
        )
        await self._reply(
            user_id,
            event.respond,
            self._format_panel(
                self._ui_text("admin_runtime_settings_title", language),
                self._ui_text("admin_runtime_reset_done", language),
                "✅",
            ),
            buttons=self._admin_panel_buttons(user_id, language),
            sticker_kind="info",
        )

    async def handle_broadcast_input(self, event: Any) -> None:
        user_id = event.sender_id
        language = self._language_for_user(user_id)
        broadcast_text = (event.raw_text or "").strip()
        if not broadcast_text:
            await self._reply(
                user_id,
                event.respond,
                self._format_panel(
                    self._ui_text("error_title", language),
                    self._ui_text("admin_broadcast_body", language),
                    "⚠️",
                ),
                sticker_kind="warning",
            )
            return

        self.storage.save_state(user_id, state="idle")
        await self._reply(
            user_id,
            event.respond,
            self._format_panel(
                self._ui_text("admin_broadcast_title", language),
                self._ui_text("admin_broadcast_started", language),
                "📣",
            ),
            sticker_kind="info",
        )

        sent_count = 0
        failed_count = 0
        safe_broadcast_text = html.escape(broadcast_text)
        for row in self.storage.list_all_users():
            target_user_id = int(row.get("user_id") or 0)
            if target_user_id <= 0:
                continue
            try:
                await self._send_message(
                    target_user_id,
                    safe_broadcast_text,
                    buttons=self.main_menu_buttons(self._language_for_user(target_user_id)),
                    sticker_kind="info",
                )
                sent_count += 1
            except Exception:
                failed_count += 1

        self.storage.log_event(
            user_id=user_id,
            event_type="broadcast_sent",
            details={
                "sent_count": sent_count,
                "failed_count": failed_count,
            },
        )
        await self._send_message(
            user_id,
            self._format_panel(
                self._ui_text("admin_broadcast_title", language),
                self._ui_text("admin_broadcast_done", language).format(
                    sent_count=sent_count,
                    failed_count=failed_count,
                ),
                "✅",
            ),
            buttons=self._admin_panel_buttons(user_id, language),
            sticker_kind="info",
        )

    async def handle_message(self, event: Any) -> None:
        raw_text = event.raw_text or ""
        text = raw_text.strip()
        user_id = event.sender_id
        state = self.storage.get_state(user_id)
        language = self._language_from_state(state)
        has_file = getattr(getattr(event, "message", None), "file", None) is not None

        if text == "/start":
            return
        if not text and not (state["state"] == "waiting_session_fragments" and has_file):
            return

        if self._is_admin_command(text):
            await self.handle_admin_command(user_id, responder=event.respond)
            return

        if state["state"] == self._admin_search_state_name("history") and self._is_admin_user(user_id):
            await self.handle_admin_search_input(event, "history")
            return

        if state["state"] == self._admin_search_state_name("users") and self._is_admin_user(user_id):
            await self.handle_admin_search_input(event, "users")
            return

        if state["state"] == "waiting_admin_runtime_update" and self._is_super_admin_user(user_id):
            await self.handle_runtime_setting_input(event)
            return

        if state["state"] == "waiting_admin_runtime_reset" and self._is_super_admin_user(user_id):
            await self.handle_runtime_setting_reset_input(event)
            return

        if state["state"] == "waiting_admin_broadcast" and self._is_super_admin_user(user_id):
            await self.handle_broadcast_input(event)
            return

        if text in self._button_variants("support"):
            await self.handle_support(user_id, responder=event.respond)
            return

        if text in self._button_variants("language"):
            await self._send_language_selector(
                user_id,
                responder=event.respond,
                language=language or "fa",
            )
            return

        if self._is_chatgpt_command(text):
            if not language:
                await self._send_language_selector(
                    user_id,
                    responder=event.respond,
                    language="fa",
                )
                return

            self.storage.log_event(
                user_id=user_id,
                event_type="chatgpt_command",
                details="activation_flow_requested",
            )
            if state["state"] == "processing_order":
                await self._reply(
                    user_id,
                    event.respond,
                    self._format_panel(
                        self._ui_text("order_in_progress_title", language),
                        self._render_key("in_progress_message", language=language),
                        "⏳",
                    ),
                    buttons=self.flow_menu_buttons(language),
                    sticker_kind="processing",
                )
            else:
                await self.start_activation_flow(user_id, responder=event.respond)
            return

        if not language:
            await self._send_language_selector(user_id, responder=event.respond, language="fa")
            return

        if text in self._button_variants("cancel"):
            if state["state"] == "processing_order":
                await self._reply(
                    user_id,
                    event.respond,
                    self._format_panel(
                        self._ui_text("order_in_progress_title", language),
                        self._render_key("in_progress_message", language=language),
                        "⏳",
                    ),
                    buttons=self.flow_menu_buttons(language),
                    sticker_kind="processing",
                )
            else:
                await self.cancel_flow(user_id, responder=event.respond)
            return

        if text in self._button_variants("renew"):
            if state["state"] == "processing_order":
                await self._reply(
                    user_id,
                    event.respond,
                    self._format_panel(
                        self._ui_text("order_in_progress_title", language),
                        self._render_key("in_progress_message", language=language),
                        "⏳",
                    ),
                    buttons=self.flow_menu_buttons(language),
                    sticker_kind="processing",
                )
            else:
                await self.start_activation_flow(user_id, responder=event.respond)
            return

        if text in self._button_variants("history"):
            if state["state"] == "processing_order":
                await self._reply(
                    user_id,
                    event.respond,
                    self._format_panel(
                        self._ui_text("order_in_progress_title", language),
                        self._render_key("in_progress_message", language=language),
                        "⏳",
                    ),
                    buttons=self.flow_menu_buttons(language),
                    sticker_kind="processing",
                )
            else:
                await self.send_user_history(user_id, responder=event.respond)
            return

        if state["state"] == "waiting_activation_code":
            await self.handle_activation_code_input(event, raw_text)
            return

        if state["state"] == "waiting_session_fragments":
            session_text = await self._extract_session_input(event, raw_text)
            await self.handle_session_fragment(event, session_text)
            return

        if state["state"] == "processing_order":
            await self._reply(
                user_id,
                event.respond,
                self._format_panel(
                    self._ui_text("order_in_progress_title", language),
                    self._render_key("in_progress_message", language=language),
                    "⏳",
                ),
                buttons=self.flow_menu_buttons(language),
                sticker_kind="processing",
            )
            return

        await self._send_main_menu(user_id, responder=event.respond, title_key="main_menu_title")

    async def handle_callback(self, event: Any) -> None:
        user_id = event.sender_id
        data = bytes(event.data or b"")
        state = self.storage.get_state(user_id)
        language = self._language_from_state(state) or "fa"

        if data.startswith(b"admin:"):
            await self.handle_admin_callback(event, data)
            return

        if data.startswith(b"set_language:"):
            selected = data.split(b":", 1)[1].decode("utf-8", errors="ignore")
            language = "en" if selected == "en" else "fa"
            self.storage.save_state(user_id, language=language)
            self.storage.log_event(
                user_id=user_id,
                event_type="language_changed",
                details={"language": language},
            )
            await event.answer(self._ui_text("language_changed_notice", language))
            await self._send_main_menu(user_id)
            return

        if data == b"cancel_flow":
            await event.answer(self._ui_text("request_cancelled", language))
            await self.cancel_flow(user_id)
            return

        if data == b"confirm_subscription":
            if state["state"] != "waiting_subscription_confirm":
                await event.answer(self._ui_text("request_inactive", language), alert=True)
                return

            self.storage.save_state(user_id, state="waiting_final_confirm")
            self.storage.log_event(
                user_id=user_id,
                event_type="subscription_warning_confirmed",
                details="confirmed",
            )
            await event.answer(self._ui_text("request_confirmed", language))
            await self.send_final_confirmation(user_id)
            return

        if data == b"confirm_final":
            if state["state"] != "waiting_final_confirm":
                await event.answer(self._ui_text("request_inactive", language), alert=True)
                return

            await event.answer(self._ui_text("submitting_order_notice", language))
            await self.begin_order_processing(user_id)
            return

        if data == b"change_account":
            if state["state"] not in {
                "waiting_session_fragments",
                "waiting_subscription_confirm",
                "waiting_final_confirm",
            }:
                await event.answer(self._ui_text("request_inactive", language), alert=True)
                return

            self.cancel_session_task(user_id)
            self.storage.save_state(
                user_id,
                state="waiting_session_fragments",
                session_fragments=[],
                session_started_at="",
                session_email="",
                session_plan_type="",
                raw_session="",
                order_id=None,
            )
            self.storage.log_event(
                user_id=user_id,
                event_type="change_account_requested",
                details="waiting_session_fragments",
            )
            await event.answer(self._ui_text("change_account_notice", language))
            await self.send_request_session_prompt(user_id)
            return

        if data == b"retry_order":
            if state["state"] != "waiting_retry_order":
                await event.answer(self._ui_text("request_inactive", language), alert=True)
                return

            await event.answer(self._ui_text("submitting_order_notice", language))
            await self.begin_order_processing(user_id)
            return

        if data == b"retry_activation_check":
            if state["state"] != "waiting_activation_code" or not str(state.get("activation_code") or "").strip():
                await event.answer(self._ui_text("request_inactive", language), alert=True)
                return
            await event.answer(self._ui_text("retry_button_text", language))
            await self._submit_activation_code_check(
                user_id=user_id,
                activation_code=str(state.get("activation_code") or ""),
            )
            return

        if data.startswith(b"userhistory:view:"):
            view_state = self._parse_user_history_view_data(data)
            if not view_state:
                await event.answer(self._ui_text("invalid_request", language), alert=True)
                return
            await event.answer()
            text, buttons = self._build_user_history_view(
                user_id=user_id,
                language=language,
                page=view_state["page"],
                per_page=view_state["per_page"],
                status_filter=view_state["filter_key"],
                sort_key=view_state["sort_key"],
            )
            await event.edit(text, buttons=buttons, parse_mode="html", link_preview=False)
            return

        await event.answer(self._ui_text("invalid_request", language), alert=True)

    async def handle_admin_callback(self, event: Any, data: bytes) -> None:
        user_id = event.sender_id
        if not self._is_admin_user(user_id):
            language = self._language_for_user(user_id)
            await event.answer(self._ui_text("admin_unauthorized", language), alert=True)
            return

        language = self._language_for_user(user_id)
        if data == b"admin:noop":
            await event.answer()
            return

        if data == b"admin:panel":
            self.storage.save_state(user_id, state="idle")
            await event.answer(self._ui_text("admin_title", language))
            await self._edit_admin_message(
                event,
                self._build_admin_panel_text(language),
                buttons=self._admin_panel_buttons(user_id, language),
            )
            return

        if data == b"admin:history":
            self._save_admin_view_context(
                user_id,
                "history",
                page=1,
                per_page=20,
                filter_key="all",
                sort_key="newest",
                search_query="",
                state_name="idle",
            )
            await event.answer(self._ui_text("admin_history_title", language))
            await self._show_admin_orders_view(
                event,
                user_id=user_id,
                language=language,
                page=1,
                per_page=20,
                status_filter="all",
                sort_key="newest",
                search_query="",
            )
            return

        if data == b"admin:users":
            self._save_admin_view_context(
                user_id,
                "users",
                page=1,
                per_page=20,
                filter_key="all",
                sort_key="last_seen",
                search_query="",
                state_name="idle",
            )
            await event.answer(self._ui_text("admin_users_title", language))
            await self._show_admin_users_view(
                event,
                user_id=user_id,
                language=language,
                page=1,
                per_page=20,
                filter_key="all",
                sort_key="last_seen",
                search_query="",
            )
            return

        if data == b"admin:search_orders":
            await event.answer(self._ui_text("admin_search_orders_button_text", language))
            await self.send_admin_search_prompt(user_id, "history")
            return

        if data == b"admin:search_users":
            await event.answer(self._ui_text("admin_search_users_button_text", language))
            await self.send_admin_search_prompt(user_id, "users")
            return

        if data == b"admin:notifications":
            self.storage.save_state(user_id, state="idle")
            await event.answer(self._ui_text("admin_notifications_title", language))
            await self._edit_admin_message(
                event,
                self._build_notification_settings_text(language),
                buttons=self._notification_settings_buttons(language),
            )
            return

        if data == b"admin:runtime":
            if not self._is_super_admin_user(user_id):
                await event.answer(self._ui_text("admin_super_admin_only", language), alert=True)
                return
            self.storage.save_state(user_id, state="idle")
            await event.answer(self._ui_text("admin_runtime_settings_title", language))
            await self._edit_admin_message(
                event,
                self._format_panel(
                    self._ui_text("admin_runtime_settings_title", language),
                    self._ui_text("admin_runtime_settings_body", language),
                    "⚙️",
                ),
                buttons=[
                    [
                        Button.inline(
                            f"✏️ {self._ui_text('admin_runtime_edit_button_text', language)}",
                            data=b"admin:runtime_edit",
                        ),
                        Button.inline(
                            f"🧹 {self._ui_text('admin_runtime_reset_button_text', language)}",
                            data=b"admin:runtime_reset",
                        ),
                    ],
                    [
                        Button.inline(
                            f"⬅️ {self._ui_text('admin_back_button_text', language)}",
                            data=b"admin:panel",
                        )
                    ],
                ],
            )
            return

        if data == b"admin:runtime_edit":
            if not self._is_super_admin_user(user_id):
                await event.answer(self._ui_text("admin_super_admin_only", language), alert=True)
                return
            self.storage.save_state(user_id, state="waiting_admin_runtime_update")
            await event.answer(self._ui_text("admin_runtime_prompt_title", language))
            await self._edit_admin_message(
                event,
                self._format_panel(
                    self._ui_text("admin_runtime_prompt_title", language),
                    self._ui_text("admin_runtime_prompt_body", language),
                    "✏️",
                ),
                buttons=[[
                    Button.inline(
                        f"⬅️ {self._ui_text('admin_back_button_text', language)}",
                        data=b"admin:runtime",
                    )
                ]],
            )
            return

        if data == b"admin:runtime_reset":
            if not self._is_super_admin_user(user_id):
                await event.answer(self._ui_text("admin_super_admin_only", language), alert=True)
                return
            self.storage.save_state(user_id, state="waiting_admin_runtime_reset")
            await event.answer(self._ui_text("admin_runtime_reset_prompt_title", language))
            await self._edit_admin_message(
                event,
                self._format_panel(
                    self._ui_text("admin_runtime_reset_prompt_title", language),
                    self._ui_text("admin_runtime_reset_prompt_body", language),
                    "🧹",
                ),
                buttons=[[
                    Button.inline(
                        f"⬅️ {self._ui_text('admin_back_button_text', language)}",
                        data=b"admin:runtime",
                    )
                ]],
            )
            return

        if data == b"admin:broadcast":
            if not self._is_super_admin_user(user_id):
                await event.answer(self._ui_text("admin_super_admin_only", language), alert=True)
                return
            self.storage.save_state(user_id, state="waiting_admin_broadcast")
            await event.answer(self._ui_text("admin_broadcast_title", language))
            await self._edit_admin_message(
                event,
                self._format_panel(
                    self._ui_text("admin_broadcast_title", language),
                    self._ui_text("admin_broadcast_body", language),
                    "📣",
                ),
                buttons=[[
                    Button.inline(
                        f"⬅️ {self._ui_text('admin_back_button_text', language)}",
                        data=b"admin:panel",
                    )
                ]],
            )
            return

        if data == b"admin:clear_search:history":
            context = self._get_admin_view_context(user_id, "history")
            await event.answer(self._ui_text("admin_clear_search_button_text", language))
            await self._show_admin_orders_view(
                event,
                user_id=user_id,
                language=language,
                page=1,
                per_page=context["per_page"],
                status_filter=context["filter_key"],
                sort_key=context["sort_key"],
                search_query="",
            )
            return

        if data == b"admin:clear_search:users":
            context = self._get_admin_view_context(user_id, "users")
            await event.answer(self._ui_text("admin_clear_search_button_text", language))
            await self._show_admin_users_view(
                event,
                user_id=user_id,
                language=language,
                page=1,
                per_page=context["per_page"],
                filter_key=context["filter_key"],
                sort_key=context["sort_key"],
                search_query="",
            )
            return

        if data.startswith(b"admin:toggle:"):
            key = data.decode("utf-8", errors="ignore").split(":", 2)[2]
            toggles = self.storage.get_notification_settings()
            if key not in toggles:
                await event.answer(self._ui_text("invalid_request", language), alert=True)
                return
            new_value = not toggles[key]
            self.storage.set_setting_bool(key, new_value)
            self.storage.log_event(
                user_id=user_id,
                event_type="admin_notification_toggled",
                details={"setting": key, "enabled": new_value},
            )
            await event.answer(
                f"{self._ui_text('admin_status_on', language) if new_value else self._ui_text('admin_status_off', language)}"
            )
            await self._edit_admin_message(
                event,
                self._build_notification_settings_text(language),
                buttons=self._notification_settings_buttons(language),
            )
            return

        if data.startswith(b"admin:view:"):
            view_state = self._parse_admin_view_data(data)
            if not view_state:
                await event.answer(self._ui_text("invalid_request", language), alert=True)
                return
            await event.answer()
            if view_state["view_kind"] == "history":
                context = self._get_admin_view_context(user_id, "history")
                await self._show_admin_orders_view(
                    event,
                    user_id=user_id,
                    language=language,
                    page=view_state["page"],
                    per_page=view_state["per_page"],
                    status_filter=view_state["filter_key"],
                    sort_key=view_state["sort_key"],
                    search_query=context["search_query"],
                )
                return
            if view_state["view_kind"] == "users":
                context = self._get_admin_view_context(user_id, "users")
                await self._show_admin_users_view(
                    event,
                    user_id=user_id,
                    language=language,
                    page=view_state["page"],
                    per_page=view_state["per_page"],
                    filter_key=view_state["filter_key"],
                    sort_key=view_state["sort_key"],
                    search_query=context["search_query"],
                )
                return
            await event.answer(self._ui_text("invalid_request", language), alert=True)
            return

        if data == b"admin:export_orders":
            await event.answer(self._ui_text("admin_export_orders_button_text", language))
            await self.send_orders_export(user_id)
            return

        if data == b"admin:export_users":
            await event.answer(self._ui_text("admin_export_users_button_text", language))
            await self.send_users_export(user_id)
            return

        if data == b"admin:close":
            await event.answer(self._ui_text("admin_closed", language))
            await self._edit_admin_message(
                event,
                self._format_panel(
                    self._ui_text("admin_title", language),
                    self._ui_text("admin_closed", language),
                    "🧹",
                ),
            )
            return

        await event.answer(self._ui_text("invalid_request", language), alert=True)

    async def handle_admin_command(self, user_id: int, *, responder: Any | None = None) -> None:
        if not self._is_admin_user(user_id):
            language = self._language_for_user(user_id) or "fa"
            if responder is None:
                await self._send_message(
                    user_id,
                    self._format_panel(
                        self._ui_text("error_title", language),
                        self._ui_text("admin_unauthorized", language),
                        "⛔️",
                    ),
                    sticker_kind="warning",
                )
                return
            await self._reply(
                user_id,
                responder,
                self._format_panel(
                    self._ui_text("error_title", language),
                    self._ui_text("admin_unauthorized", language),
                    "⛔️",
                ),
                sticker_kind="warning",
            )
            return

        current_state = self.storage.get_state(user_id)
        if not self._language_from_state(current_state):
            self.storage.save_state(user_id, language="fa")
        self.storage.save_state(user_id, state="idle")
        self.storage.set_admin_chat_id(user_id)
        self.storage.log_event(
            user_id=user_id,
            event_type="admin_panel_opened",
            details={"bot_index": self.settings.bot_index},
        )
        await self.send_admin_panel(user_id, responder=responder)

    async def handle_support(self, user_id: int, *, responder: Any | None = None) -> None:
        state = self.storage.get_state(user_id)
        language = self._language_for_user(user_id, state)
        text = self._format_panel(
            self._ui_text("support_title", language),
            self._render_key("support_message", language=language),
            "🛟",
        )
        if responder is None:
            await self._send_message(
                user_id,
                text,
                buttons=self.menu_buttons_for_state(state["state"], language),
                sticker_kind="support",
            )
            return
        await self._reply(
            user_id,
            responder,
            text,
            buttons=self.menu_buttons_for_state(state["state"], language),
            sticker_kind="support",
        )

    async def cancel_flow(self, user_id: int, *, responder: Any | None = None) -> None:
        state = self.storage.get_state(user_id)
        language = self._language_for_user(user_id, state)
        await self.reset_user_flow(user_id, force=True)
        self.storage.log_event(
            user_id=user_id, event_type="flow_cancelled", details="cancelled"
        )
        text = self._format_panel(
            self._ui_text("flow_cancelled_title", language),
            self._render_key("cancelled_message", language=language),
            "🧹",
        )
        if responder is None:
            await self._send_message(
                user_id,
                text,
                buttons=self.main_menu_buttons(language),
                sticker_kind="info",
            )
            return
        await self._reply(
            user_id,
            responder,
            text,
            buttons=self.main_menu_buttons(language),
            sticker_kind="info",
        )

    async def start_activation_flow(
        self,
        user_id: int,
        *,
        responder: Any | None = None,
    ) -> None:
        language = self._language_for_user(user_id)
        self.cancel_session_task(user_id)
        self.storage.save_state(
            user_id,
            state="waiting_activation_code",
            activation_code="",
            activation_app_name="",
            activation_product_name="",
            activation_payload="",
            session_fragments=[],
            session_started_at="",
            session_email="",
            session_plan_type="",
            raw_session="",
            order_id=None,
        )
        self.storage.log_event(
            user_id=user_id,
            event_type="activation_flow_started",
            details="waiting_activation_code",
        )
        self.logger.info(
            "activation_flow_started user=%s",
            self._log_user_identity(user_id),
        )
        text = self._format_panel(
            self._ui_text("activation_title", language),
            self._render_key("request_activation_code_message", language=language),
            "🔑",
        )
        if responder is None:
            await self._send_message(
                user_id,
                text,
                buttons=self.flow_menu_buttons(language),
                sticker_kind="activation",
            )
            return
        await self._reply(
            user_id,
            responder,
            text,
            buttons=self.flow_menu_buttons(language),
            sticker_kind="activation",
        )

    async def send_request_session_prompt(self, user_id: int) -> None:
        language = self._language_for_user(user_id)
        await self._send_message(
            user_id,
            self._format_panel(
                self._ui_text("request_session_title", language),
                self._render_key("request_session_message", language=language),
                "🧾",
            ),
            link_preview=False,
            buttons=self.flow_menu_buttons(language),
            sticker_kind="info",
        )

    async def _submit_activation_code_check(
        self,
        *,
        user_id: int,
        activation_code: str,
        responder: Any | None = None,
    ) -> None:
        language = self._language_for_user(user_id)
        self.storage.save_state(user_id, activation_code=activation_code)
        checking_text = self._format_panel(
            self._ui_text("please_wait_title", language),
            self._render_key("activation_checking_message", language=language),
            "⏳",
        )
        if responder is None:
            checking_message = await self._send_message(user_id, checking_text)
        else:
            checking_message = await self._reply(
                user_id,
                responder,
                checking_text,
                buttons=self.flow_menu_buttons(language),
            )

        self.storage.log_event(
            user_id=user_id,
            event_type="activation_code_received",
            details={"activation_code": activation_code},
        )
        self.logger.info(
            "activation_code_received user=%s code=%s",
            self._log_user_identity(user_id),
            self._mask_value(activation_code, keep_start=8, keep_end=6),
        )
        try:
            payload = await asyncio.to_thread(
                self._api_client().check_activation_code,
                activation_code,
            )
        except ApiError as exc:
            retryable = self._is_retryable_api_error(exc)
            self.logger.warning(
                "activation_check_failed user=%s path=%s status=%s request_id=%s body=%r",
                self._log_user_identity(user_id),
                exc.path,
                exc.status_code,
                exc.request_id,
                exc.response_body,
            )
            self.storage.log_event(
                user_id=user_id,
                event_type="activation_code_check_error",
                details={
                    "error": exc.to_dict(),
                    "activation_code": activation_code,
                    "retryable": retryable,
                },
            )
            await self._delete_message(user_id, checking_message)
            if retryable:
                error_body = self._render_key(
                    "activation_check_error_message",
                    language=language,
                    error=self._sanitize_api_text(str(exc)),
                    support_hint=self._ui_text("retry_now_hint", language),
                )
            else:
                error_body = self._render_key(
                    "activation_check_error_message",
                    language=language,
                    error=self._sanitize_api_text(str(exc)),
                )
            if responder is None:
                await self._send_message(
                    user_id,
                    self._format_panel(
                        self._ui_text("activation_check_error_title", language),
                        error_body,
                        "⚠️",
                    ),
                    buttons=self._retry_activation_buttons(language)
                    if retryable
                    else self.flow_menu_buttons(language),
                    sticker_kind="warning",
                )
            else:
                await self._reply(
                    user_id,
                    responder,
                    self._format_panel(
                        self._ui_text("activation_check_error_title", language),
                        error_body,
                        "⚠️",
                    ),
                    buttons=self._retry_activation_buttons(language)
                    if retryable
                    else self.flow_menu_buttons(language),
                    sticker_kind="warning",
                )
            return

        if payload.get("used"):
            self.storage.log_event(
                user_id=user_id,
                event_type="activation_code_used",
                details=payload,
            )
            await self._delete_message(user_id, checking_message)
            if responder is None:
                await self._send_message(
                    user_id,
                    self._format_panel(
                        self._ui_text("activation_used_title", language),
                        self._render_key("activation_used_message", language=language),
                        "⚠️",
                    ),
                    buttons=self.flow_menu_buttons(language),
                    sticker_kind="warning",
                )
            else:
                await self._reply(
                    user_id,
                    responder,
                    self._format_panel(
                        self._ui_text("activation_used_title", language),
                        self._render_key("activation_used_message", language=language),
                        "⚠️",
                    ),
                    buttons=self.flow_menu_buttons(language),
                    sticker_kind="warning",
                )
            return

        resolved_activation_code = self._resolve_activation_code(activation_code, payload)
        unknown_value = self._ui_text("unknown_value", language)
        app_name = str(payload.get("app_name") or unknown_value)
        product_name = str(payload.get("app_product_name") or unknown_value)
        self.storage.save_state(
            user_id,
            state="waiting_session_fragments",
            activation_code=resolved_activation_code,
            activation_app_name=app_name,
            activation_product_name=product_name,
            activation_payload=json.dumps(payload, ensure_ascii=False),
            session_fragments=[],
            session_started_at="",
            session_email="",
            session_plan_type="",
            raw_session="",
            order_id=None,
        )
        self.storage.log_event(
            user_id=user_id,
            event_type="activation_code_valid",
            details={**payload, "resolved_activation_code": resolved_activation_code},
        )
        self.logger.info(
            "activation_code_valid user=%s product=%s app=%s code=%s",
            self._log_user_identity(user_id),
            product_name,
            app_name,
            self._mask_value(resolved_activation_code, keep_start=8, keep_end=6),
        )
        await self._delete_message(user_id, checking_message)
        result_text = self._format_panel(
            self._ui_text("activation_checked_title", language),
            self._render_key(
                "activation_info_message",
                language=language,
                app_name=app_name,
                app_product_name=product_name,
                usage_status=self._runtime_text_value("usage_status_ready_text", language),
            ),
            "✅",
        )
        if responder is None:
            await self._send_message(
                user_id,
                result_text,
                buttons=self.flow_menu_buttons(language),
                sticker_kind="success",
            )
        else:
            await self._reply(
                user_id,
                responder,
                result_text,
                buttons=self.flow_menu_buttons(language),
                sticker_kind="success",
            )
        await self.send_request_session_prompt(user_id)

    async def handle_activation_code_input(self, event: Any, text: str) -> None:
        user_id = event.sender_id
        language = self._language_for_user(user_id)
        activation_code = self._normalize_activation_code(text)
        if not activation_code:
            await self._reply(
                user_id,
                event.respond,
                self._format_panel(
                    self._ui_text("activation_invalid_title", language),
                    self._render_key("activation_invalid_message", language=language),
                    "⚠️",
                ),
                buttons=self.flow_menu_buttons(language),
                sticker_kind="warning",
            )
            return

        if not self._is_valid_activation_code(activation_code):
            await self._reply(
                user_id,
                event.respond,
                self._build_activation_code_format_message(language),
                buttons=self.flow_menu_buttons(language),
                sticker_kind="warning",
            )
            return

        await self._submit_activation_code_check(
            user_id=user_id,
            activation_code=activation_code,
            responder=event.respond,
        )

    async def handle_session_fragment(self, event: Any, text: str) -> None:
        user_id = event.sender_id
        language = self._language_for_user(user_id)
        state = self.storage.get_state(user_id)
        fragments = list(state["session_fragments"])
        is_first_fragment = len(fragments) == 0
        fragments.append(text)

        self.storage.save_state(
            user_id,
            session_fragments=fragments,
            session_started_at=state["session_started_at"] or utc_now_iso(),
        )
        self.storage.log_event(
            user_id=user_id,
            event_type="session_fragment_received",
            details={"fragment_count": len(fragments)},
        )
        self.logger.info(
            "session_fragment_received user=%s fragment_count=%s",
            self._log_user_identity(user_id),
            len(fragments),
        )

        if is_first_fragment:
            checking_message = await self._send_message(
                user_id,
                self._format_panel(
                    self._ui_text("please_wait_title", language),
                    self._render_key("session_checking_message", language=language),
                    "⏳",
                ),
            )
            message_id = getattr(checking_message, "id", None)
            if message_id is not None:
                self.session_status_messages[user_id] = message_id

        if len(fragments) >= self._runtime_int_value("session_max_messages"):
            self.cancel_session_task(user_id)
            await self.finalize_session_fragments(user_id)
            return

        if len(fragments) == 1:
            self.session_tasks[user_id] = asyncio.create_task(
                self._finalize_session_after_window(user_id)
            )

    async def _finalize_session_after_window(self, user_id: int) -> None:
        try:
            await asyncio.sleep(self._runtime_int_value("session_window_seconds"))
            await self.finalize_session_fragments(user_id)
        except asyncio.CancelledError:  # pragma: no cover - timing path
            return
        except Exception as exc:  # pragma: no cover - runtime fallback
            self.logger.exception(
                "session_finalize_unhandled user=%s error=%s",
                self._log_user_identity(user_id),
                exc,
            )
            self.storage.log_event(
                user_id=user_id,
                event_type="session_finalize_unhandled",
                details={"error": str(exc)},
            )
            await self._clear_session_status_message(user_id)
            language = self._language_for_user(user_id)
            await self._send_message(
                user_id,
                self._format_panel(
                    self._ui_text("session_check_error_title", language),
                    self._render_runtime_template(
                        self._ui_text("session_check_unexpected", language),
                        language=language,
                    ),
                    "⚠️",
                ),
                link_preview=False,
                buttons=self.flow_menu_buttons(language),
            )

    async def finalize_session_fragments(self, user_id: int) -> None:
        self.cancel_session_task(user_id, preserve_current=True)
        state = self.storage.get_state(user_id)
        language = self._language_for_user(user_id, state)
        if state["state"] != "waiting_session_fragments":
            return

        raw_session = ""
        try:
            raw_session = combine_session_fragments(list(state["session_fragments"]))
            if not raw_session:
                return
            session_data = SessionData.parse(raw_session)
        except SessionValidationError as exc:
            await self._clear_session_status_message(user_id)
            self.storage.save_state(
                user_id,
                state="waiting_session_fragments",
                session_fragments=[],
                session_started_at="",
                session_email="",
                session_plan_type="",
                raw_session="",
            )
            self.storage.log_event(
                user_id=user_id,
                event_type="session_invalid",
                details={"error": str(exc), "raw_session": raw_session},
            )
            self.logger.warning(
                "session_invalid user=%s error=%s",
                self._log_user_identity(user_id),
                exc,
            )
            await self._send_message(
                user_id,
                self._format_panel(
                    self._ui_text("session_invalid_title", language),
                    f"{html.escape(str(exc))}\n\n"
                    f"{self._render_key('session_invalid_message', language=language)}",
                    "⚠️",
                ),
                link_preview=False,
                buttons=self._change_account_buttons(language),
                sticker_kind="warning",
            )
            return
        except Exception as exc:  # pragma: no cover - runtime fallback
            await self._clear_session_status_message(user_id)
            self.storage.save_state(
                user_id,
                state="waiting_session_fragments",
                session_fragments=[],
                session_started_at="",
                session_email="",
                session_plan_type="",
                raw_session="",
            )
            self.storage.log_event(
                user_id=user_id,
                event_type="session_finalize_error",
                details={"error": str(exc), "raw_session": raw_session},
            )
            self.logger.exception(
                "session_finalize_error user=%s error=%s",
                self._log_user_identity(user_id),
                exc,
            )
            await self._send_message(
                user_id,
                self._format_panel(
                    self._ui_text("session_check_error_title", language),
                    self._render_runtime_template(
                        self._ui_text("session_check_unexpected", language),
                        language=language,
                    ),
                    "⚠️",
                ),
                link_preview=False,
                buttons=self.flow_menu_buttons(language),
                sticker_kind="warning",
            )
            return

        await self._clear_session_status_message(user_id)
        next_state = (
            "waiting_subscription_confirm"
            if session_data.has_active_subscription
            else "waiting_final_confirm"
        )
        self.storage.save_state(
            user_id,
            state=next_state,
            session_fragments=[],
            session_started_at="",
            session_email=session_data.email,
            session_plan_type=session_data.plan_type,
            raw_session=session_data.raw_text,
        )
        self.storage.log_event(
            user_id=user_id,
            event_type="session_valid",
            details={
                "email": session_data.email,
                "plan_type": session_data.plan_type,
                "raw_session": session_data.raw_text,
            },
        )
        self.logger.info(
            "session_valid user=%s email=%s plan_type=%s",
            self._log_user_identity(user_id),
            session_data.email,
            session_data.plan_type,
        )

        if session_data.has_active_subscription:
            await self._send_message(
                user_id,
                self._format_panel(
                    self._ui_text("subscription_warning_title", language),
                    self._render_key("subscription_warning_message", language=language),
                    "⚠️",
                ),
                buttons=[
                    [
                        Button.inline(
                            self._button_label("confirm", language),
                            data=b"confirm_subscription",
                        ),
                        Button.inline(
                            self._inline_label("change_account", language),
                            data=b"change_account",
                        ),
                    ],
                    [Button.inline(self._button_label("cancel", language), data=b"cancel_flow")],
                ],
                sticker_kind="warning",
            )
            return

        await self.send_final_confirmation(user_id)

    async def send_final_confirmation(self, user_id: int) -> None:
        state = self.storage.get_state(user_id)
        language = self._language_for_user(user_id, state)
        unknown_value = self._ui_text("unknown_value", language)
        await self._send_message(
            user_id,
            self._format_panel(
                self._ui_text("final_confirm_title", language),
                self._render_key(
                    "final_confirm_message",
                    language=language,
                    email=state["session_email"] or unknown_value,
                    product=state["activation_product_name"] or unknown_value,
                ),
                "🟢",
            ),
            buttons=self.confirm_buttons(b"confirm_final", language),
            sticker_kind="info",
        )

    async def begin_order_processing(self, user_id: int) -> None:
        state = self.storage.get_state(user_id)
        language = self._language_for_user(user_id, state)
        if state["state"] not in {"waiting_final_confirm", "waiting_retry_order"}:
            return

        order_id = self.storage.create_order(
            user_id=user_id,
            activation_code=state["activation_code"],
            app_name=state["activation_app_name"],
            product_name=state["activation_product_name"],
            email=state["session_email"],
            plan_type=state["session_plan_type"],
            raw_session=state["raw_session"],
            status="processing",
        )
        self.storage.save_state(user_id, state="processing_order", order_id=order_id)
        self.storage.log_event(
            user_id=user_id,
            event_type="order_processing_started",
            details={"order_id": order_id},
        )
        self.logger.info(
            "order_processing_started user=%s order_id=%s product=%s email=%s",
            self._log_user_identity(user_id),
            order_id,
            state["activation_product_name"],
            state["session_email"],
        )
        await self._send_message(
            user_id,
            self._format_panel(
                self._ui_text("processing_order_title", language),
                self._render_key("processing_order_message", language=language),
                "🚀",
            ),
            buttons=self.flow_menu_buttons(language),
            sticker_kind="processing",
        )

        existing_task = self.order_tasks.pop(user_id, None)
        if existing_task is not None:
            existing_task.cancel()

        self.order_tasks[user_id] = asyncio.create_task(
            self.process_order(
                user_id=user_id,
                order_id=order_id,
                activation_code=state["activation_code"],
                app_name=state["activation_app_name"],
                product_name=state["activation_product_name"],
                email=state["session_email"],
                raw_session=state["raw_session"],
            )
        )

    async def process_order(
        self,
        *,
        user_id: int,
        order_id: int,
        activation_code: str,
        app_name: str,
        product_name: str,
        email: str,
        raw_session: str,
    ) -> None:
        language = self._language_for_user(user_id)
        try:
            outstock_user = extract_outstock_user(raw_session)
            self.storage.log_event(
                user_id=user_id,
                event_type="order_submit_payload",
                details={
                    "order_id": order_id,
                    "user_source": "raw_session",
                    "user_length": len(outstock_user),
                },
            )
            self.logger.info(
                "order_submit_payload user=%s order_id=%s user_source=raw_session user_length=%s code=%s",
                self._log_user_identity(user_id),
                order_id,
                len(outstock_user),
                self._mask_value(activation_code, keep_start=8, keep_end=6),
            )
            task_id = await asyncio.to_thread(
                self._api_client().create_outstock_order,
                activation_code,
                outstock_user,
            )

            self.storage.update_order(
                order_id,
                task_id=task_id,
                status="submitted",
                task_result="",
            )
            self.storage.log_event(
                user_id=user_id,
                event_type="order_submitted",
                details={
                    "order_id": order_id,
                    "task_id": task_id,
                    "user_source": "raw_session",
                },
            )
            self.logger.info(
                "order_submitted user=%s order_id=%s task_id=%s user_source=raw_session",
                self._log_user_identity(user_id),
                order_id,
                task_id,
            )

            loop = asyncio.get_running_loop()
            deadline = loop.time() + self._runtime_int_value("outstock_timeout_seconds")
            while loop.time() < deadline:
                await asyncio.sleep(self._runtime_int_value("outstock_poll_seconds"))
                try:
                    result = await asyncio.to_thread(self._api_client().get_outstock_status, task_id)
                except ApiError as exc:
                    self.storage.update_order(
                        order_id,
                        status="poll_error",
                        task_result=str(exc),
                    )
                    self.storage.log_event(
                        user_id=user_id,
                        event_type="order_poll_error",
                        details={"order_id": order_id, "error": exc.to_dict()},
                    )
                    self.logger.warning(
                        "order_poll_error user=%s order_id=%s path=%s status=%s request_id=%s body=%r",
                        self._log_user_identity(user_id),
                        order_id,
                        exc.path,
                        exc.status_code,
                        exc.request_id,
                        exc.response_body,
                    )
                    await self._send_message(
                        user_id,
                        self._format_panel(
                            self._ui_text("order_poll_error_title", language),
                            self._render_key(
                                "order_poll_error_message",
                                language=language,
                                error=self._sanitize_api_text(str(exc)),
                            ),
                            "⚠️",
                        ),
                        sticker_kind="warning",
                    )
                    await self._notify_admin_activation_result(
                        order_id=order_id,
                        success=False,
                        message_text=str(exc),
                    )
                    await self.finish_user_flow(user_id)
                    return

                raw_result = json_dumps(result)
                if result.get("pending", False):
                    self.storage.update_order(
                        order_id,
                        status="pending",
                        task_result=raw_result,
                    )
                    continue

                self.storage.update_order(
                    order_id,
                    status="success" if result.get("success") else "failed",
                    task_result=raw_result,
                )
                self.storage.log_event(
                    user_id=user_id,
                    event_type="order_completed",
                    details={"order_id": order_id, "result": result},
                )
                self.logger.info(
                    "order_completed user=%s order_id=%s success=%s pending=%s",
                    self._log_user_identity(user_id),
                    order_id,
                    result.get("success"),
                    result.get("pending"),
                )
                if result.get("success"):
                    await self._notify_admin_activation_result(
                        order_id=order_id,
                        success=True,
                        message_text=str(result.get("message") or ""),
                    )
                    await self._send_message(
                        user_id,
                        self._build_order_result_panel(
                            language=language,
                            activation_code=activation_code,
                            app_name=app_name,
                            product_name=product_name,
                            email=email,
                            result=result,
                        ),
                        sticker_kind="success",
                    )
                    await self.finish_user_flow(user_id)
                    return

                self.storage.save_state(
                    user_id,
                    state="waiting_retry_order",
                    order_id=order_id,
                )
                await self._notify_admin_activation_result(
                    order_id=order_id,
                    success=False,
                    message_text=str(result.get("message") or result.get("error") or ""),
                )
                await self._send_message(
                    user_id,
                    self._build_order_result_panel(
                        language=language,
                        activation_code=activation_code,
                        app_name=app_name,
                        product_name=product_name,
                        email=email,
                        result=result,
                    ),
                    buttons=self._retry_order_buttons(language),
                    sticker_kind="warning",
                )
                return

            self.storage.update_order(order_id, status="timeout", task_result="timeout")
            self.storage.log_event(
                user_id=user_id,
                event_type="order_timeout",
                details={"order_id": order_id},
            )
            self.logger.warning(
                "order_timeout user=%s order_id=%s",
                self._log_user_identity(user_id),
                order_id,
            )
            await self._send_message(
                user_id,
                self._format_panel(
                    self._ui_text("order_timeout_title", language),
                    self._render_key("order_timeout_message", language=language),
                    "⚠️",
                ),
                sticker_kind="warning",
            )
            await self._notify_admin_activation_result(
                order_id=order_id,
                success=False,
                message_text="timeout",
            )
            await self.finish_user_flow(user_id)
        except ApiError as exc:
            self.storage.update_order(
                order_id,
                status="submit_error",
                task_result=str(exc),
            )
            self.storage.log_event(
                user_id=user_id,
                event_type="order_submit_error",
                details={"order_id": order_id, "error": exc.to_dict()},
            )
            self.logger.warning(
                "order_submit_error user=%s order_id=%s path=%s status=%s request_id=%s body=%r",
                self._log_user_identity(user_id),
                order_id,
                exc.path,
                exc.status_code,
                exc.request_id,
                exc.response_body,
            )
            await self._send_message(
                user_id,
                self._format_panel(
                    self._ui_text("order_submit_error_title", language),
                    self._render_key(
                        "order_submit_error_message",
                        language=language,
                        error=self._sanitize_api_text(str(exc)),
                    ),
                    "⚠️",
                ),
                sticker_kind="warning",
            )
            await self._notify_admin_activation_result(
                order_id=order_id,
                success=False,
                message_text=str(exc),
            )
            await self.finish_user_flow(user_id)
        except Exception as exc:  # pragma: no cover - runtime fallback
            self.logger.exception(
                "Unexpected order processing error for user=%s",
                self._log_user_identity(user_id),
            )
            self.storage.update_order(
                order_id,
                status="unexpected_error",
                task_result=str(exc),
            )
            self.storage.log_event(
                user_id=user_id,
                event_type="order_unexpected_error",
                details={"order_id": order_id, "error": str(exc)},
            )
            await self._send_message(
                user_id,
                self._format_panel(
                    self._ui_text("unexpected_error_title", language),
                    self._render_key("generic_error_message", language=language),
                    "⚠️",
                ),
                sticker_kind="warning",
            )
            await self._notify_admin_activation_result(
                order_id=order_id,
                success=False,
                message_text=str(exc),
            )
            await self.finish_user_flow(user_id)
        finally:
            self.order_tasks.pop(user_id, None)

    async def finish_user_flow(self, user_id: int) -> None:
        language = self._language_for_user(user_id)
        await self.reset_user_flow(user_id, force=True)
        await self._send_message(
            user_id,
            self._format_panel(
                self._ui_text("return_to_menu_title", language),
                self._render_key("return_to_menu_message", language=language),
                "📌",
            ),
            buttons=self.main_menu_buttons(language),
            sticker_kind="info",
        )

    async def reset_user_flow(self, user_id: int, *, force: bool = False) -> None:
        self.cancel_session_task(user_id)
        await self._clear_session_status_message(user_id)
        state = self.storage.get_state(user_id)
        if force or state["state"] != "processing_order":
            self.storage.reset_state(user_id)

    def cancel_session_task(self, user_id: int, *, preserve_current: bool = False) -> None:
        task = self.session_tasks.pop(user_id, None)
        if task is not None:
            if preserve_current and task is asyncio.current_task():
                return
            task.cancel()
