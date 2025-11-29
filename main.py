from __future__ import annotations

import tkinter as tk
from tkinter import filedialog, messagebox, ttk
import tkinter.font as tkfont

from consensus import MasterchainConsensus  # type: ignore
from platform import DigitalRublePlatform  # type: ignore


class DigitalRubleApp(tk.Tk):
    def __init__(self) -> None:
        super().__init__()
        self.title("Имитационная модель цифрового рубля")
        self.geometry("1600x900")
        # Увеличенный шрифт по умолчанию для всей программы
        default_font = tkfont.nametofont("TkDefaultFont")
        default_font.configure(size=11)
        self.option_add("*Font", default_font)
        heading_font = tkfont.nametofont("TkHeadingFont")
        heading_font.configure(size=12, weight="bold")
        self.platform = DigitalRublePlatform()
        self.notebook = ttk.Notebook(self)
        self.notebook.pack(fill=tk.BOTH, expand=True)
        self._init_state()
        self._build_tabs()
        self.refresh_all()

    def _init_state(self) -> None:
        self.user_table = None
        self.tx_table = None
        self.offline_table = None
        self.contract_table = None
        self.consensus_table = None
        self.consensus_canvas = None
        self.ledger_canvas = None
        self.block_table = None
        self.utxo_table = None
        self.activity_text = None
        self.bank_tx_table = None
        self.issuance_table = None
        self.cbr_log = None
        self.wallet_user_combo = None
        self.offline_user_combo = None
        self.offline_receiver_combo = None
        self.offline_sender_combo = None
        self.sender_combo = None
        self.receiver_combo = None
        self.channel_combo = None
        self.contract_sender_combo = None
        self.contract_receiver_combo = None
        self.contract_bank_combo = None
        self.bank_combo = None
        # переменные для анимации консенсуса и реестра
        self._consensus_anim_events = []
        self._consensus_anim_index = 0
        self._consensus_anim_job = None
        self._consensus_active_actor = None
        self._consensus_active_state = None
        self._consensus_active_event = None
        self._ledger_last_rows = []
        self._ledger_active_height = None

    def _user_type_label(self, code: str) -> str:
        mapping = {
            "INDIVIDUAL": "Физическое лицо",
            "BUSINESS": "Юридическое лицо",
            "GOVERNMENT": "Государственное учреждение",
        }
        return mapping.get(code, code)

    # region --- UI builders -------------------------------------------------------
    def _build_tabs(self) -> None:
        self._build_management_tab()
        self._build_user_tab()
        self._build_bank_tab()
        self._build_cbr_tab()
        self._build_user_data_tab()
        self._build_tx_data_tab()
        self._build_offline_tab()
        self._build_contracts_tab()
        self._build_consensus_tab()
        self._build_ledger_tab()
        self._build_activity_tab()

    def _build_management_tab(self) -> None:
        tab = ttk.Frame(self.notebook)
        self.notebook.add(tab, text="Управление")
        controls = ttk.LabelFrame(tab, text="Создание участников")
        controls.pack(fill=tk.X, padx=10, pady=10)

        ttk.Label(controls, text="Физические лица:").grid(row=0, column=0, padx=5, pady=5)
        fl_entry = ttk.Entry(controls, width=5)
        fl_entry.insert(0, "5")
        fl_entry.grid(row=0, column=1, padx=5, pady=5)

        ttk.Label(controls, text="Юридические лица:").grid(row=0, column=2, padx=5, pady=5)
        yl_entry = ttk.Entry(controls, width=5)
        yl_entry.insert(0, "3")
        yl_entry.grid(row=0, column=3, padx=5, pady=5)

        ttk.Label(controls, text="Банки (ФО):").grid(row=0, column=4, padx=5, pady=5)
        bank_entry = ttk.Entry(controls, width=5)
        bank_entry.insert(0, "2")
        bank_entry.grid(row=0, column=5, padx=5, pady=5)

        ttk.Label(controls, text="Гос.организации:").grid(row=0, column=6, padx=5, pady=5)
        gov_entry = ttk.Entry(controls, width=5)
        gov_entry.insert(0, "1")
        gov_entry.grid(row=0, column=7, padx=5, pady=5)

        def seed_entities() -> None:
            try:
                self.platform.create_banks(int(bank_entry.get()))
                self.platform.create_users(int(fl_entry.get()), "INDIVIDUAL")
                self.platform.create_users(int(yl_entry.get()), "BUSINESS")
                self.platform.create_government_institutions(int(gov_entry.get()))
                self.refresh_all()
                messagebox.showinfo("Управление", "Данные успешно сгенерированы")
            except Exception as exc:
                messagebox.showerror("Ошибка", str(exc))

        ttk.Button(controls, text="Создать", command=seed_entities).grid(
            row=0, column=8, padx=10, pady=5
        )

        def reset_entities() -> None:
            if not messagebox.askyesno(
                "Сброс модели",
                "Вы уверены, что хотите полностью очистить все данные модели?\n"
                "Будут удалены пользователи, банки, транзакции, смарт‑контракты и журналы.",
            ):
                return
            try:
                self.platform.reset_state()
                self.refresh_all()
                messagebox.showinfo("Сброс модели", "Все данные имитационной модели очищены")
            except Exception as exc:
                messagebox.showerror("Ошибка", str(exc))

        ttk.Button(controls, text="Очистить данные", command=reset_entities).grid(
            row=0, column=9, padx=10, pady=5
        )

    def _build_user_tab(self) -> None:
        tab = ttk.Frame(self.notebook)
        self.notebook.add(tab, text="Пользователь")
        tab.columnconfigure(0, weight=1)
        # единая ширина колонок для всех фреймов
        LABEL_WIDTH = 180
        FIELD_WIDTH = 300

        # 1. Создание цифрового кошелька
        wallet_frame = ttk.LabelFrame(tab, text="Создание цифрового кошелька")
        wallet_frame.grid(row=0, column=0, sticky="nsew", padx=10, pady=10)
        wallet_frame.columnconfigure(1, weight=1)

        ttk.Label(wallet_frame, text="Пользователь:", width=LABEL_WIDTH//10).grid(
            row=0, column=0, padx=5, pady=5, sticky="w"
        )
        self.wallet_user_combo = ttk.Combobox(wallet_frame, state="readonly", width=FIELD_WIDTH//10)
        self.wallet_user_combo.grid(row=0, column=1, padx=5, pady=5, sticky="ew")
        ttk.Button(wallet_frame, text="Открыть кошелек", command=self._ui_open_wallet).grid(
            row=0, column=2, padx=5, pady=5, sticky="ew"
        )
        ttk.Label(wallet_frame, text="Сумма конвертации:", width=LABEL_WIDTH//10).grid(
            row=1, column=0, padx=5, pady=5, sticky="w"
        )
        self.convert_amount = ttk.Entry(wallet_frame, width=FIELD_WIDTH//10)
        self.convert_amount.insert(0, "1000")
        self.convert_amount.grid(row=1, column=1, padx=5, pady=5, sticky="ew")
        ttk.Button(wallet_frame, text="Пополнить ЦР", command=self._ui_convert_funds).grid(
            row=1, column=2, padx=5, pady=5, sticky="ew"
        )

        # 2. Онлайн транзакции
        online_frame = ttk.LabelFrame(tab, text="Онлайн транзакции")
        online_frame.grid(row=1, column=0, sticky="nsew", padx=10, pady=10)
        online_frame.columnconfigure(1, weight=1)

        ttk.Label(online_frame, text="Отправитель:", width=LABEL_WIDTH//10).grid(
            row=0, column=0, padx=5, pady=5, sticky="w"
        )
        self.sender_combo = ttk.Combobox(online_frame, state="readonly", width=FIELD_WIDTH//10)
        self.sender_combo.grid(row=0, column=1, padx=5, pady=5, sticky="ew")

        ttk.Label(online_frame, text="Получатель:", width=LABEL_WIDTH//10).grid(
            row=1, column=0, padx=5, pady=5, sticky="w"
        )
        self.receiver_combo = ttk.Combobox(online_frame, state="readonly", width=FIELD_WIDTH//10)
        self.receiver_combo.grid(row=1, column=1, padx=5, pady=5, sticky="ew")

        ttk.Label(online_frame, text="Тип перевода:", width=LABEL_WIDTH//10).grid(
            row=2, column=0, padx=5, pady=5, sticky="w"
        )
        self.channel_combo = ttk.Combobox(
            online_frame,
            values=["C2C", "C2B", "B2C", "B2B", "G2B", "B2G", "C2G", "G2C"],
            state="readonly",
            width=FIELD_WIDTH//10,
        )
        self.channel_combo.current(0)
        self.channel_combo.grid(row=2, column=1, padx=5, pady=5, sticky="ew")
        self.channel_combo.bind("<<ComboboxSelected>>", self._on_channel_change)

        ttk.Label(online_frame, text="Сумма:", width=LABEL_WIDTH//10).grid(
            row=3, column=0, padx=5, pady=5, sticky="w"
        )
        self.online_amount = ttk.Entry(online_frame, width=FIELD_WIDTH//10)
        self.online_amount.insert(0, "300")
        self.online_amount.grid(row=3, column=1, padx=5, pady=5, sticky="ew")
        ttk.Button(online_frame, text="Перевести", command=self._ui_online_tx).grid(
            row=4, column=0, columnspan=2, padx=5, pady=10, sticky="ew"
        )

        # 3. Открытие оффлайн кошелька
        offline_wallet_frame = ttk.LabelFrame(tab, text="Открытие оффлайн кошелька")
        offline_wallet_frame.grid(row=2, column=0, sticky="nsew", padx=10, pady=10)
        offline_wallet_frame.columnconfigure(1, weight=1)

        ttk.Label(offline_wallet_frame, text="Пользователь:", width=LABEL_WIDTH//10).grid(
            row=0, column=0, padx=5, pady=5, sticky="w"
        )
        self.offline_user_combo = ttk.Combobox(offline_wallet_frame, state="readonly", width=FIELD_WIDTH//10)
        self.offline_user_combo.grid(row=0, column=1, padx=5, pady=5, sticky="ew")
        ttk.Button(
            offline_wallet_frame, text="Открыть оффлайн кошелек", command=self._ui_open_offline
        ).grid(row=0, column=2, padx=5, pady=5, sticky="ew")
        ttk.Label(offline_wallet_frame, text="Сумма пополнения:", width=LABEL_WIDTH//10).grid(
            row=1, column=0, padx=5, pady=5, sticky="w"
        )
        self.offline_amount = ttk.Entry(offline_wallet_frame, width=FIELD_WIDTH//10)
        self.offline_amount.insert(0, "500")
        self.offline_amount.grid(row=1, column=1, padx=5, pady=5, sticky="ew")
        ttk.Button(offline_wallet_frame, text="Пополнить оффлайн", command=self._ui_fund_offline).grid(
            row=1, column=2, padx=5, pady=5, sticky="ew"
        )

        # 4. Создание оффлайн транзакции
        offline_tx_frame = ttk.LabelFrame(tab, text="Создание оффлайн транзакции")
        offline_tx_frame.grid(row=3, column=0, sticky="nsew", padx=10, pady=10)
        offline_tx_frame.columnconfigure(1, weight=1)

        ttk.Label(offline_tx_frame, text="Отправитель:", width=LABEL_WIDTH//10).grid(
            row=0, column=0, padx=5, pady=5, sticky="w"
        )
        self.offline_sender_combo = ttk.Combobox(offline_tx_frame, state="readonly", width=FIELD_WIDTH//10)
        self.offline_sender_combo.grid(row=0, column=1, padx=5, pady=5, sticky="ew")

        ttk.Label(offline_tx_frame, text="Получатель:", width=LABEL_WIDTH//10).grid(
            row=1, column=0, padx=5, pady=5, sticky="w"
        )
        self.offline_receiver_combo = ttk.Combobox(offline_tx_frame, state="readonly", width=FIELD_WIDTH//10)
        self.offline_receiver_combo.grid(row=1, column=1, padx=5, pady=5, sticky="ew")

        ttk.Label(offline_tx_frame, text="Сумма:", width=LABEL_WIDTH//10).grid(
            row=2, column=0, padx=5, pady=5, sticky="w"
        )
        self.offline_tx_amount = ttk.Entry(offline_tx_frame, width=FIELD_WIDTH//10)
        self.offline_tx_amount.insert(0, "200")
        self.offline_tx_amount.grid(row=2, column=1, padx=5, pady=5, sticky="ew")
        ttk.Button(
            offline_tx_frame, text="Создать оффлайн-транзакцию", command=self._ui_offline_tx
        ).grid(row=2, column=2, padx=5, pady=5, sticky="ew")

        # 5. Создание смарт-контракта
        contract_frame = ttk.LabelFrame(tab, text="Создание смарт-контракта")
        contract_frame.grid(row=4, column=0, sticky="nsew", padx=10, pady=10)
        contract_frame.columnconfigure(1, weight=1)

        ttk.Label(contract_frame, text="ФЛ-отправитель:", width=LABEL_WIDTH//10).grid(
            row=0, column=0, padx=5, pady=5, sticky="w"
        )
        self.contract_sender_combo = ttk.Combobox(contract_frame, state="readonly", width=FIELD_WIDTH//10)
        self.contract_sender_combo.grid(row=0, column=1, padx=5, pady=5, sticky="ew")

        ttk.Label(contract_frame, text="ЮЛ/Гос получатель:", width=LABEL_WIDTH//10).grid(
            row=1, column=0, padx=5, pady=5, sticky="w"
        )
        self.contract_receiver_combo = ttk.Combobox(contract_frame, state="readonly", width=FIELD_WIDTH//10)
        self.contract_receiver_combo.grid(row=1, column=1, padx=5, pady=5, sticky="ew")

        ttk.Label(contract_frame, text="Банк:", width=LABEL_WIDTH//10).grid(
            row=2, column=0, padx=5, pady=5, sticky="w"
        )
        self.contract_bank_combo = ttk.Combobox(contract_frame, state="readonly", width=FIELD_WIDTH//10)
        self.contract_bank_combo.grid(row=2, column=1, padx=5, pady=5, sticky="ew")

        ttk.Label(contract_frame, text="Сумма:", width=LABEL_WIDTH//10).grid(
            row=3, column=0, padx=5, pady=5, sticky="w"
        )
        self.contract_amount = ttk.Entry(contract_frame, width=FIELD_WIDTH//10)
        self.contract_amount.insert(0, "1000")
        self.contract_amount.grid(row=3, column=1, padx=5, pady=5, sticky="ew")

        ttk.Label(contract_frame, text="Описание:", width=LABEL_WIDTH//10).grid(
            row=4, column=0, padx=5, pady=5, sticky="w"
        )
        self.contract_description = ttk.Entry(contract_frame, width=FIELD_WIDTH//10)
        self.contract_description.insert(0, "Автоплатеж за коммунальные услуги")
        self.contract_description.grid(row=4, column=1, padx=5, pady=5, sticky="ew")

        ttk.Button(
            contract_frame, text="Создать смарт-контракт", command=self._ui_create_contract
        ).grid(row=5, column=0, columnspan=2, pady=10, sticky="ew")
        ttk.Button(
            contract_frame, text="Исполнить запланированные", command=self._ui_run_contracts
        ).grid(row=6, column=0, columnspan=2, pady=5, sticky="ew")

    def _build_bank_tab(self) -> None:
        tab = ttk.Frame(self.notebook)
        self.notebook.add(tab, text="Финансовая организация")
        tab.columnconfigure(0, weight=1)
        tab.rowconfigure(2, weight=1)

        request_frame = ttk.LabelFrame(tab, text="Запрос эмиссии")
        request_frame.grid(row=0, column=0, sticky="ew", padx=10, pady=10)

        ttk.Label(request_frame, text="Банк:").grid(row=0, column=0, padx=5, pady=5)
        self.bank_combo = ttk.Combobox(request_frame, state="readonly")
        self.bank_combo.grid(row=0, column=1, padx=5, pady=5)

        ttk.Label(request_frame, text="Сумма ЦР:").grid(row=0, column=2, padx=5, pady=5)
        self.emission_amount = ttk.Entry(request_frame)
        self.emission_amount.insert(0, "100000")
        self.emission_amount.grid(row=0, column=3, padx=5, pady=5)
        ttk.Button(
            request_frame, text="Отправить запрос", command=self._ui_request_emission
        ).grid(row=0, column=4, padx=5, pady=5)

        ttk.Label(tab, text="Транзакции, прошедшие через банк").grid(
            row=1, column=0, sticky="w", padx=10
        )
        table_frame = ttk.Frame(tab)
        table_frame.grid(row=2, column=0, sticky="nsew")
        self.bank_tx_table = self._make_table(
            table_frame,
            ["ID", "Отправитель", "Получатель", "Тип", "Сумма"],
            stretch=True,
        )
        ttk.Button(tab, text="Обновить данные", command=self.refresh_all).grid(
            row=3, column=0, pady=5
        )

    def _build_cbr_tab(self) -> None:
        tab = ttk.Frame(self.notebook)
        self.notebook.add(tab, text="Центральный банк")
        tab.columnconfigure(0, weight=1)

        ttk.Label(tab, text="Запросы на эмиссию").grid(row=0, column=0, sticky="w", padx=10, pady=5)
        self.issuance_table = self._make_table(
            tab,
            ["ID", "Банк", "Сумма", "Статус"],
            row=1,
            column=0,
        )

        action_frame = ttk.Frame(tab)
        action_frame.grid(row=2, column=0, sticky="ew", padx=10, pady=5)

        ttk.Button(action_frame, text="Одобрить", command=lambda: self._ui_process_emission(True)).pack(
            side=tk.LEFT, padx=5
        )
        ttk.Button(action_frame, text="Отклонить", command=lambda: self._ui_process_emission(False)).pack(
            side=tk.LEFT, padx=5
        )

        ttk.Label(tab, text="Журнал событий ЦБ и системы").grid(
            row=3, column=0, sticky="w", padx=10, pady=5
        )
        self.cbr_log = tk.Text(tab, height=18)
        self.cbr_log.grid(row=4, column=0, sticky="nsew", padx=10, pady=5)
        tab.rowconfigure(4, weight=1)

    def _build_user_data_tab(self) -> None:
        tab = ttk.Frame(self.notebook)
        self.notebook.add(tab, text="Данные о пользователях")
        columns = [
            "ID",
            "Тип",
            "Баланс безнал",
            "Статус цифрового",
            "Баланс цифрового",
            "Статус оффлайн",
            "Баланс оффлайн",
            "Активация оффлайн",
            "Деактивация",
        ]
        self.user_table = self._make_table(tab, columns, stretch=True)

    def _build_tx_data_tab(self) -> None:
        tab = ttk.Frame(self.notebook)
        self.notebook.add(tab, text="Данные о транзакциях")
        columns = [
            "ID",
            "Отправитель",
            "Получатель",
            "Тип",
            "Сумма",
            "Время",
            "Банк",
        ]
        self.tx_table = self._make_table(tab, columns, stretch=True)

    def _build_offline_tab(self) -> None:
        tab = ttk.Frame(self.notebook)
        self.notebook.add(tab, text="Оффлайн-транзакции")
        tab.rowconfigure(0, weight=1)
        tab.columnconfigure(0, weight=1)
        table_frame = ttk.Frame(tab)
        table_frame.grid(row=0, column=0, sticky="nsew")
        columns = [
            "ID",
            "Отправитель",
            "Получатель",
            "Сумма",
            "Банк",
            "Время",
            "Статус",
        ]
        self.offline_table = self._make_table(table_frame, columns, stretch=True)
        ttk.Button(tab, text="Синхронизировать", command=self._ui_sync_offline).grid(
            row=1, column=0, pady=5
        )

    def _build_contracts_tab(self) -> None:
        tab = ttk.Frame(self.notebook)
        self.notebook.add(tab, text="Смарт-контракты")
        tab.rowconfigure(0, weight=1)
        tab.columnconfigure(0, weight=1)
        columns = [
            "ID",
            "Отправитель",
            "Получатель",
            "Банк",
            "Назначение",
            "Дата исполнения",
            "Сумма",
            "Исполнен",
        ]
        self.contract_table = self._make_table(tab, columns, stretch=True)
        ttk.Button(tab, text="Обновить данные", command=self.refresh_all).grid(
            row=1, column=0, pady=5
        )

    def _build_consensus_tab(self) -> None:
        tab = ttk.Frame(self.notebook)
        self.notebook.add(tab, text="Консенсус")
        tab.columnconfigure(0, weight=1)
        tab.columnconfigure(1, weight=1)
        tab.rowconfigure(0, weight=0)
        tab.rowconfigure(1, weight=0)
        tab.rowconfigure(2, weight=1)

        self.consensus_canvas = tk.Canvas(tab, height=280, bg="white")
        self.consensus_canvas.grid(row=0, column=0, columnspan=2, sticky="ew", padx=10, pady=10)

        self.ledger_canvas = tk.Canvas(tab, height=180, bg="white")
        self.ledger_canvas.grid(row=1, column=0, columnspan=2, sticky="ew", padx=10, pady=5)

        ttk.Label(tab, text="События консенсуса", font=("TkDefaultFont", 11, "bold")).grid(
            row=2, column=0, columnspan=2, sticky="w", padx=10, pady=(10, 5)
        )
        columns = ["Блок/Транзакция", "Событие", "Узел", "Состояние", "Время"]
        self.consensus_table = self._make_table(tab, columns, row=3, column=0, columnspan=2, stretch=True)

        btn_frame = ttk.Frame(tab)
        btn_frame.grid(row=4, column=0, columnspan=2, pady=5)
        ttk.Button(
            btn_frame,
            text="Обновить визуализацию",
            command=self._ui_refresh_consensus,
        ).pack(side=tk.LEFT, padx=5)

    def _build_ledger_tab(self) -> None:
        tab = ttk.Frame(self.notebook)
        self.notebook.add(tab, text="Распределенный реестр")
        tab.columnconfigure(0, weight=1)
        tab.rowconfigure(1, weight=1)
        tab.rowconfigure(3, weight=1)

        ttk.Label(tab, text="Блоки распределенного реестра", font=("TkDefaultFont", 11, "bold")).grid(
            row=0, column=0, sticky="w", padx=10, pady=(10, 5)
        )
        table_frame = ttk.Frame(tab)
        table_frame.grid(row=1, column=0, sticky="nsew", padx=10, pady=5)
        table_frame.columnconfigure(0, weight=1)
        table_frame.rowconfigure(0, weight=1)

        self.block_table = self._make_table(
            table_frame,
            ["Номер блока", "Хеш блока", "Хеш родителя", "Узел", "Время создания"],
            stretch=True,
        )

        ttk.Label(tab, text="UTXO (Незатраченные выходы транзакций)", font=("TkDefaultFont", 11, "bold")).grid(
            row=2, column=0, sticky="w", padx=10, pady=(10, 5)
        )
        utxo_frame = ttk.Frame(tab)
        utxo_frame.grid(row=3, column=0, sticky="nsew", padx=10, pady=5)
        utxo_frame.columnconfigure(0, weight=1)
        utxo_frame.rowconfigure(0, weight=1)
        self.utxo_table = self._make_table(
            utxo_frame,
            ["ID", "Владелец", "Сумма", "Статус", "Транзакция создания", "Транзакция списания"],
            stretch=True,
        )

        ttk.Button(tab, text="Экспортировать реестр", command=self._ui_export_registry).grid(
            row=4, column=0, pady=5
        )

    def _build_activity_tab(self) -> None:
        tab = ttk.Frame(self.notebook)
        self.notebook.add(tab, text="Информация о этапах")
        container = ttk.Frame(tab)
        container.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        y_scroll = ttk.Scrollbar(container, orient="vertical")
        y_scroll.pack(side=tk.RIGHT, fill=tk.Y)
        self.activity_text = tk.Text(container, yscrollcommand=y_scroll.set)
        self.activity_text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        y_scroll.config(command=self.activity_text.yview)

    # endregion -------------------------------------------------------------------

    # region --- UI helpers --------------------------------------------------------
    def _translate_tx_type(self, tx_type: str) -> str:
        """Переводит тип транзакции на русский."""
        mapping = {
            "ONLINE": "Онлайн",
            "OFFLINE": "Оффлайн",
            "EXCHANGE": "Обмен",
            "CONTRACT": "Смарт-контракт",
        }
        return mapping.get(tx_type, tx_type)

    def _translate_channel(self, channel: str) -> str:
        """Переводит канал транзакции на русский."""
        mapping = {
            "C2C": "ФЛ → ФЛ",
            "C2B": "ФЛ → ЮЛ",
            "B2C": "ЮЛ → ФЛ",
            "B2B": "ЮЛ → ЮЛ",
            "G2B": "Гос → ЮЛ",
            "B2G": "ЮЛ → Гос",
            "C2G": "ФЛ → Гос",
            "G2C": "Гос → ФЛ",
            "FIAT2DR": "Пополнение цифрового кошелька",
            "OFFLINE_FUND": "Пополнение оффлайн кошелька",
        }
        return mapping.get(channel, channel)

    def _translate_wallet_status(self, status: str) -> str:
        """Переводит статус кошелька на русский."""
        mapping = {
            "OPEN": "Открыт",
            "CLOSED": "Закрыт",
        }
        return mapping.get(status, status)

    def _translate_status(self, status: str) -> str:
        """Переводит статус на русский."""
        mapping = {
            "UNSPENT": "Незатрачен",
            "SPENT": "Затрачен",
            "CONFIRMED": "Подтверждена",
            "OFFLINE_BUFFER": "Оффлайн буфер",
            "SCHEDULED": "Запланирован",
            "EXECUTED": "Исполнен",
            "PENDING": "Ожидает",
            "APPROVED": "Одобрено",
            "REJECTED": "Отклонено",
            "ОФФЛАЙН": "Оффлайн",
            "ПОСТУПИЛО В ОБРАБОТКУ": "В обработке",
            "ОБРАБОТАНА": "Обработана",
            "КОНФЛИКТ": "Конфликт",
        }
        return mapping.get(status, status)

    def _translate_consensus_state(self, state: str) -> str:
        """Переводит состояние консенсуса на русский."""
        mapping = {
            "PRE-PREPARE": "Пред-подготовка",
            "PREPARE": "Подготовка",
            "PREPARE_MSG": "Сообщение подготовки",
            "COMMIT": "Фиксация",
            "COMMIT_MSG": "Сообщение фиксации",
            "FINALIZE": "Завершение",
            "SYNC": "Синхронизация",
            "TX": "Транзакция",
            "LAG": "Задержка",
            "FAULT": "Ошибка",
        }
        return mapping.get(state, state)

    def _make_table(
        self,
        parent,
        columns,
        row: int = 0,
        column: int = 0,
        columnspan: int = 1,
        stretch: bool = False,
    ):
        tree = ttk.Treeview(parent, columns=columns, show="headings")
        for col in columns:
            tree.heading(col, text=col)
            tree.column(col, width=150 if not stretch else 120, anchor="center")
        tree.grid(row=row, column=column, columnspan=columnspan, sticky="nsew", padx=10, pady=10)
        if stretch:
            parent.rowconfigure(row, weight=1)
            parent.columnconfigure(column, weight=1)
        scrollbar = ttk.Scrollbar(parent, orient="vertical", command=tree.yview)
        tree.configure(yscrollcommand=scrollbar.set)
        scrollbar.grid(row=row, column=column + columnspan, sticky="ns")
        return tree

    def refresh_all(self) -> None:
        self._refresh_user_lists()
        self._refresh_tables()
        self._refresh_consensus_canvas()

    def _refresh_user_lists(self) -> None:
        users = self.platform.list_users()
        formatted = [
            f"{u['id']} | {u['name']} ({self._user_type_label(u['user_type'])})" for u in users
        ]
        # сохранить текущие выбранные значения, чтобы не сбрасывать выбор пользователя
        combos = [
            self.wallet_user_combo,
            self.offline_user_combo,
            self.offline_sender_combo,
            self.contract_sender_combo,
            self.offline_receiver_combo,
        ]
        for combo in combos:
            if combo:
                old = combo.get()
                combo["values"] = formatted
                if old and old in formatted:
                    combo.set(old)
                elif not combo.get() and formatted:
                    combo.current(0)
        receivers = [
            f"{u['id']} | {u['name']} ({self._user_type_label(u['user_type'])})"
            for u in users
            if u["user_type"] in {"BUSINESS", "GOVERNMENT"}
        ]
        if self.contract_receiver_combo:
            old = self.contract_receiver_combo.get()
            self.contract_receiver_combo["values"] = receivers
            if old and old in receivers:
                self.contract_receiver_combo.set(old)
            elif not self.contract_receiver_combo.get() and receivers:
                self.contract_receiver_combo.current(0)
        banks = self.platform.list_banks()
        bank_values = [f"{b['id']} | {b['name']}" for b in banks]
        for combo in [self.bank_combo, self.contract_bank_combo]:
            if combo:
                old = combo.get()
                combo["values"] = bank_values
                if old and old in bank_values:
                    combo.set(old)
                elif not combo.get() and bank_values:
                    combo.current(0)
        # обновить списки для онлайн транзакций с учётом выбранного типа канала
        self._refresh_online_combos()

    def _refresh_tables(self) -> None:
        def clear(tree):
            if tree:
                for item in tree.get_children():
                    tree.delete(item)

        if self.user_table:
            clear(self.user_table)
            for u in self.platform.list_users():
                self.user_table.insert(
                    "",
                    tk.END,
                    values=(
                        u["id"],
                        self._user_type_label(u["user_type"]),
                        f"{u['fiat_balance']:.2f}",
                        self._translate_wallet_status(u["wallet_status"]),
                        f"{u['digital_balance']:.2f}",
                        self._translate_wallet_status(u["offline_status"]),
                        f"{u['offline_balance']:.2f}",
                        u.get("offline_activated_at", "") or "-",
                        u.get("offline_expires_at", "") or "-",
                    ),
                )

        if self.tx_table:
            clear(self.tx_table)
            for tx in self.platform.get_transactions():
                sender = self.platform.get_user(tx["sender_id"])
                receiver = self.platform.get_user(tx["receiver_id"])
                bank = self.platform._get_bank(tx["bank_id"])
                self.tx_table.insert(
                    "",
                    tk.END,
                    values=(
                        tx["id"],
                        sender["name"],
                        receiver["name"],
                        "Смарт-контракт" if tx['tx_type'] == "CONTRACT" else (
                            self._translate_channel(tx['channel']) if tx['tx_type'] == "EXCHANGE" else tx['channel']
                        ),
                        f"{tx['amount']:.2f}",
                        tx["timestamp"],
                        bank["name"],
                    ),
                )

        if self.offline_table:
            clear(self.offline_table)
            for tx in self.platform.get_offline_transactions():
                sender = self.platform.get_user(tx["sender_id"])
                receiver = self.platform.get_user(tx["receiver_id"])
                bank = self.platform._get_bank(tx["bank_id"])
                self.offline_table.insert(
                    "",
                    tk.END,
                    values=(
                        tx["id"],
                        sender["name"],
                        receiver["name"],
                        f"{tx['amount']:.2f}",
                        bank["name"],
                        tx["timestamp"],
                        self._translate_status(tx["offline_status"]),
                    ),
                )

        if self.contract_table:
            clear(self.contract_table)
            for sc in self.platform.get_smart_contracts():
                creator = self.platform.get_user(sc["creator_id"])
                beneficiary = self.platform.get_user(sc["beneficiary_id"])
                bank = self.platform._get_bank(sc["bank_id"])
                if sc["status"] == "EXECUTED":
                    exec_state = sc.get("last_execution") or "Исполнен"
                else:
                    exec_state = "Ожидает исполнения"
                self.contract_table.insert(
                    "",
                    tk.END,
                    values=(
                        sc["id"],
                        creator["name"],
                        beneficiary["name"],
                        bank["name"],
                        sc["description"],
                        sc["next_execution"],
                        f"{sc['amount']:.2f}",
                        exec_state,
                    ),
                )

        if self.block_table:
            clear(self.block_table)
            rows = self.platform.db.execute(
                "SELECT * FROM blocks ORDER BY height ASC", fetchall=True
            )
            for row in rows:
                self.block_table.insert(
                    "",
                    tk.END,
                    values=(
                        row["height"],
                        row["hash"][:12] + "...",
                        (row["previous_hash"] or "")[:12] + "...",
                        row["signer"],
                        row["timestamp"],
                    ),
                )

        if self.utxo_table:
            clear(self.utxo_table)
            rows = self.platform.db.execute(
                """
                SELECT id, owner_id, amount, status, created_tx_id, COALESCE(spent_tx_id, '-') AS spent_tx_id
                FROM utxos
                ORDER BY created_at DESC
                """,
                fetchall=True,
            )
            for row in rows or []:
                try:
                    owner = self.platform.get_user(row["owner_id"])
                    owner_name = owner["name"]
                except (ValueError, KeyError):
                    owner_name = f"ID {row['owner_id']}"
                self.utxo_table.insert(
                    "",
                    tk.END,
                    values=(
                        row["id"],
                        owner_name,
                        f"{row['amount']:.2f}",
                        self._translate_status(row["status"]),
                        row["created_tx_id"][:12] + "..." if row["created_tx_id"] != "-" else "-",
                        row["spent_tx_id"][:12] + "..." if row["spent_tx_id"] != "-" else "-",
                    ),
                )

        if self.bank_tx_table:
            clear(self.bank_tx_table)
            for tx in self.platform.get_transactions():
                sender = self.platform.get_user(tx["sender_id"])
                receiver = self.platform.get_user(tx["receiver_id"])
                self.bank_tx_table.insert(
                    "",
                    tk.END,
                    values=(
                        tx["id"],
                        sender["name"],
                        receiver["name"],
                        self._translate_channel(tx['channel']) if tx['tx_type'] == "EXCHANGE" else tx['channel'],
                        f"{tx['amount']:.2f}",
                    ),
                )

        if self.issuance_table:
            clear(self.issuance_table)
            rows = self.platform.db.execute(
                """
                SELECT i.id, b.name as bank_name, i.amount, i.status
                FROM issuance_requests i
                JOIN banks b ON b.id = i.bank_id
                ORDER BY i.requested_at DESC
                """,
                fetchall=True,
            )
            for row in rows:
                self.issuance_table.insert(
                    "",
                    tk.END,
                    values=(row["id"], row["bank_name"], f"{row['amount']:.2f}", self._translate_status(row["status"])),
                )

        if self.consensus_table:
            clear(self.consensus_table)
            events = self.platform.consensus.get_recent_events()
            for event in events:
                self.consensus_table.insert(
                    "",
                    tk.END,
                    values=(
                        event.block_hash[:12],
                        event.event,
                        event.actor,
                        self._translate_consensus_state(event.state),
                        event.created_at,
                    ),
                )

        if self.activity_text:
            self.activity_text.delete("1.0", tk.END)
            self.activity_text.tag_configure("conflict", foreground="red")
            self.activity_text.tag_configure("separator", foreground="gray", font=("TkDefaultFont", 9, "bold"))
            entries = self.platform.get_activity_log()
            prev_context = None
            prev_time = None
            for entry in entries:
                # Добавляем разделитель при смене контекста или большом разрыве во времени
                current_context = entry.get("context", "")
                current_time = entry.get("created_at", "")
                if prev_context and prev_context != current_context:
                    separator = f"\n{'='*80}\n[{current_time}] === {current_context} ===\n{'-'*80}\n"
                    self.activity_text.insert(tk.END, separator, "separator")
                elif prev_time and current_time:
                    # Проверяем разрыв во времени (больше 2 секунд)
                    try:
                        from datetime import datetime
                        prev_dt = datetime.fromisoformat(prev_time.replace("Z", "+00:00"))
                        curr_dt = datetime.fromisoformat(current_time.replace("Z", "+00:00"))
                        if (curr_dt - prev_dt).total_seconds() > 2:
                            separator = f"\n{'-'*80}\n"
                            self.activity_text.insert(tk.END, separator, "separator")
                    except:
                        pass
                line = f"[{entry['created_at']}] {entry['stage']} | {entry['actor']} -> {entry['details']}\n"
                lower = (entry["stage"] + entry["details"]).lower()
                if "конфликт" in lower or "двойной трат" in lower:
                    self.activity_text.insert(tk.END, line, "conflict")
                else:
                    self.activity_text.insert(tk.END, line)
                prev_context = current_context
                prev_time = current_time
            self.activity_text.see(tk.END)

        if self.cbr_log:
            self.cbr_log.delete("1.0", tk.END)
            for entry in self.platform.get_activity_log(limit=200):
                self.cbr_log.insert(
                    tk.END,
                    f"{entry['created_at']} | {entry['context']} | {entry['stage']} | {entry['details']}\n",
                )

    def _refresh_consensus_canvas(self) -> None:
        canvas = self.consensus_canvas
        if not canvas:
            return
        canvas.delete("all")
        nodes = self.platform.consensus.get_nodes()
        width = int(canvas.winfo_width() or 1200)
        leader = nodes[0]
        bank_nodes = nodes[1:]
        # если запущена анимация – берём активный узел из неё,
        # иначе подсвечиваем узел последнего события
        active_actor = self._consensus_active_actor
        if active_actor is None:
            recent_events = self.platform.consensus.get_recent_events(limit=1)
            active_actor = recent_events[0].actor if recent_events else None

        # координаты лидера (ЦБ) в верхней строке (опущены ниже, чтобы не накладываться на подписи)
        leader_x = width // 2
        leader_y = 120
        # подсветка в зависимости от состояния (нормальный / LAG / FAULT)
        if active_actor == leader and self._consensus_active_state in {"FAULT"}:
            leader_fill = "#ef4444"
        elif active_actor == leader and self._consensus_active_state in {"LAG"}:
            leader_fill = "#facc15"
        elif active_actor == leader:
            leader_fill = "#10b981"
        else:
            leader_fill = "#2563eb"
        canvas.create_oval(
            leader_x - 45, leader_y - 45, leader_x + 45, leader_y + 45, fill=leader_fill, outline="#0f172a", width=2
        )
        canvas.create_text(leader_x, leader_y, text=leader, fill="black", width=140)

        # банки (ФО) второй строкой, стрелки от ЦБ к каждому (опущены ниже)
        if bank_nodes:
            # Улучшенный расчет spacing для размещения всех узлов
            min_spacing = 120
            calculated_spacing = width // (len(bank_nodes) + 1)
            spacing = max(calculated_spacing, min_spacing)
            # Если узлов слишком много, используем прокрутку или уменьшаем размер узлов
            if spacing < min_spacing:
                spacing = min_spacing
            y_banks = 220
            for idx, node in enumerate(bank_nodes, start=1):
                x = spacing * idx
                # Если узел выходит за границы, центрируем распределение
                if x + 35 > width - 10:
                    # Пересчитываем с центрированием
                    total_width = spacing * len(bank_nodes)
                    start_x = (width - total_width) // 2
                    x = start_x + spacing * (idx - 1) + spacing // 2
                if node == active_actor and self._consensus_active_state == "FAULT":
                    fill_color = "#ef4444"
                elif node == active_actor and self._consensus_active_state == "LAG":
                    fill_color = "#facc15"
                elif node == active_actor:
                    fill_color = "#10b981"
                else:
                    fill_color = "#2563eb"
                canvas.create_oval(
                    x - 35, y_banks - 35, x + 35, y_banks + 35, fill=fill_color, outline="#0f172a", width=2
                )
                canvas.create_text(x, y_banks, text=node, fill="black", width=120)
                canvas.create_line(
                    leader_x,
                    leader_y + 45,
                    x,
                    y_banks - 35,
                    arrow=tk.LAST,
                    fill="#059669",
                    width=2,
                )
        stats = self.platform.consensus.stats()
        subtitle = self._consensus_active_event or ""
        canvas.create_text(
            width // 2,
            15,
            text=f"Раунды консенсуса: {stats['rounds']} | Последний блок: {stats['last_block']}",
            fill="black",
        )
        # кворум PREPARE/COMMIT для последнего блока
        if stats["last_block"] != "-":
            qrows = self.platform.db.execute(
                """
                SELECT state, COUNT(*) as cnt
                FROM consensus_events
                WHERE block_hash = ? AND state IN ('PREPARE_MSG','COMMIT_MSG')
                GROUP BY state
                """,
                (stats["last_block"],),
                fetchall=True,
            )
            counts = {row["state"]: row["cnt"] for row in qrows or []}
            total_banks = max(len(nodes) - 1, 1)
            prepare_q = counts.get("PREPARE_MSG", 0)
            commit_q = counts.get("COMMIT_MSG", 0)
            canvas.create_text(
                width // 2,
                30,
                text=f"Кворум PREPARE: {prepare_q}/{total_banks} | Кворум COMMIT: {commit_q}/{total_banks}",
                fill="#4b5563",
            )
        if subtitle:
            canvas.create_text(
                width // 2,
                48,
                text=subtitle,
                fill="#4b5563",
            )

        # Отрисовка визуализации распределённого реестра на второй канве
        ledger_canvas = self.ledger_canvas
        if ledger_canvas:
            ledger_canvas.delete("all")
            lwidth = int(ledger_canvas.winfo_width() or 1200)
            rows = self.platform.db.execute(
                "SELECT height, hash, previous_hash FROM blocks ORDER BY height DESC LIMIT 8",
                fetchall=True,
            )
            if not rows:
                ledger_canvas.create_text(
                    lwidth // 2,
                    40,
                    text="Блоки реестра ещё не созданы",
                    fill="black",
                )
            else:
                rows = list(reversed(rows))
                self._ledger_last_rows = rows
                count = len(rows)
                spacing_l = max(lwidth // (count + 1), 120)
                y_l = 80
                prev_x = None
                prev_y = None
                for idx, row in enumerate(rows, start=1):
                    x = spacing_l * idx
                    x0, y0, x1, y1 = x - 60, y_l - 30, x + 60, y_l + 30
                    is_active = row["height"] == self._ledger_active_height
                    fill_color = "#fde68a" if is_active else "#eff6ff"
                    outline_color = "#92400e" if is_active else "#1d4ed8"
                    ledger_canvas.create_rectangle(
                        x0, y0, x1, y1, fill=fill_color, outline=outline_color, width=2
                    )
                    ledger_canvas.create_text(
                        x,
                        y_l - 10,
                        text=f"Блок {row['height']}",
                        fill="black",
                    )
                    ledger_canvas.create_text(
                        x,
                        y_l + 10,
                        text=row["hash"][:10] + "...",
                        fill="#4b5563",
                    )
                    if prev_x is not None:
                        ledger_canvas.create_line(
                            prev_x + 60, prev_y, x - 60, y_l, arrow=tk.LAST, fill="#6b7280"
                        )
                    prev_x, prev_y = x, y_l

    # endregion -------------------------------------------------------------------

    # region --- Button callbacks --------------------------------------------------
    def _refresh_online_combos(self) -> None:
        """Обновить списки отправителя и получателя с учётом выбранного типа перевода."""
        if not self.sender_combo or not self.receiver_combo:
            return
        channel = self.channel_combo.get() if self.channel_combo else "C2C"
        mapping = {
            "C2C": ("INDIVIDUAL", "INDIVIDUAL"),
            "C2B": ("INDIVIDUAL", "BUSINESS"),
            "B2C": ("BUSINESS", "INDIVIDUAL"),
            "B2B": ("BUSINESS", "BUSINESS"),
            "G2B": ("GOVERNMENT", "BUSINESS"),
            "B2G": ("BUSINESS", "GOVERNMENT"),
            "C2G": ("INDIVIDUAL", "GOVERNMENT"),
            "G2C": ("GOVERNMENT", "INDIVIDUAL"),
        }
        sender_type, receiver_type = mapping.get(channel, ("INDIVIDUAL", "INDIVIDUAL"))
        senders = [
            f"{u['id']} | {u['name']} ({u['user_type']})"
            for u in self.platform.list_users(sender_type)
        ]
        receivers = [
            f"{u['id']} | {u['name']} ({u['user_type']})"
            for u in self.platform.list_users(receiver_type)
        ]
        old_sender = self.sender_combo.get()
        self.sender_combo["values"] = senders
        if old_sender in senders:
            self.sender_combo.set(old_sender)
        elif senders:
            self.sender_combo.current(0)

        old_receiver = self.receiver_combo.get()
        self.receiver_combo["values"] = receivers
        if old_receiver in receivers:
            self.receiver_combo.set(old_receiver)
        elif receivers:
            self.receiver_combo.current(0)

    def _on_channel_change(self, event=None) -> None:
        self._refresh_online_combos()
    def _ui_refresh_consensus(self) -> None:
        """Обновить таблицу и перезапустить анимацию консенсуса/реестра."""
        self.refresh_all()
        self._start_consensus_animation()

    def _start_consensus_animation(self) -> None:
        """Запуск анимации по последнему раунду консенсуса."""
        if self._consensus_anim_job is not None:
            self.after_cancel(self._consensus_anim_job)
            self._consensus_anim_job = None
        stats = self.platform.consensus.stats()
        last_block = stats.get("last_block")
        if not last_block or last_block == "-":
            # нет блоков – просто перерисуем статичную схему
            self._consensus_anim_events = []
            self._consensus_anim_index = 0
            self._consensus_active_actor = None
            self._consensus_active_state = None
            self._consensus_active_event = None
            self._ledger_active_height = None
            self._refresh_consensus_canvas()
            return
        rows = self.platform.db.execute(
            """
            SELECT block_hash, event, actor, state, created_at
            FROM consensus_events
            WHERE block_hash = ?
            ORDER BY id ASC
            """,
            (last_block,),
            fetchall=True,
        )
        self._consensus_anim_events = [dict(r) for r in rows] if rows else []
        self._consensus_anim_index = 0
        self._run_consensus_animation_step()

    def _run_consensus_animation_step(self) -> None:
        if not self._consensus_anim_events or not self.consensus_canvas:
            self._consensus_anim_job = None
            return
        event = self._consensus_anim_events[self._consensus_anim_index]
        self._consensus_active_actor = event["actor"]
        self._consensus_active_state = event["state"]
        self._consensus_active_event = event["event"]
        # для анимации реестра подсвечиваем блоки по кругу
        if self._ledger_last_rows:
            idx = self._consensus_anim_index % len(self._ledger_last_rows)
            self._ledger_active_height = self._ledger_last_rows[idx]["height"]
        else:
            self._ledger_active_height = None
        self._refresh_consensus_canvas()
        self._consensus_anim_index = (self._consensus_anim_index + 1) % len(
            self._consensus_anim_events
        )
        self._consensus_anim_job = self.after(800, self._run_consensus_animation_step)

    def _selected_id(self, value: str) -> int:
        if not value:
            raise ValueError("Выберите участника из списка")
        return int(value.split("|")[0].strip())

    def _ui_open_wallet(self) -> None:
        try:
            user_id = self._selected_id(self.wallet_user_combo.get())
            user = self.platform.get_user(user_id)
            already_open = user["wallet_status"] == "OPEN"
            self.platform.open_digital_wallet(user_id)
            self.refresh_all()
            if already_open:
                messagebox.showinfo("Цифровой кошелек", f"У пользователя {user['name']} кошелек уже открыт")
            else:
                messagebox.showinfo("Цифровой кошелек", f"Цифровой кошелек для {user['name']} успешно открыт")
        except Exception as exc:
            messagebox.showerror("Ошибка", str(exc))

    def _ui_convert_funds(self) -> None:
        try:
            user_id = self._selected_id(self.wallet_user_combo.get())
            amount = float(self.convert_amount.get())
            self.platform.exchange_to_digital(user_id, amount)
            self.refresh_all()
            user = self.platform.get_user(user_id)
            messagebox.showinfo(
                "Конвертация средств",
                f"Цифровой кошелек пользователя {user['name']} пополнен на {amount:.2f} ЦР",
            )
        except Exception as exc:
            messagebox.showerror("Ошибка", str(exc))

    def _ui_open_offline(self) -> None:
        try:
            user_id = self._selected_id(self.offline_user_combo.get())
            user = self.platform.get_user(user_id)
            already_open = user["offline_status"] == "OPEN"
            self.platform.open_offline_wallet(user_id)
            self.refresh_all()
            if already_open:
                messagebox.showinfo("Оффлайн-кошелек", f"Оффлайн-кошелек пользователя {user['name']} уже активен")
            else:
                messagebox.showinfo("Оффлайн-кошелек", f"Оффлайн-кошелек для {user['name']} активирован на 14 дней")
        except Exception as exc:
            messagebox.showerror("Ошибка", str(exc))

    def _ui_fund_offline(self) -> None:
        try:
            user_id = self._selected_id(self.offline_user_combo.get())
            amount = float(self.offline_amount.get())
            self.platform.fund_offline_wallet(user_id, amount)
            self.refresh_all()
            user = self.platform.get_user(user_id)
            messagebox.showinfo(
                "Оффлайн-кошелек",
                f"Оффлайн-кошелек пользователя {user['name']} пополнен на {amount:.2f} ЦР",
            )
        except Exception as exc:
            messagebox.showerror("Ошибка", str(exc))

    def _ui_offline_tx(self) -> None:
        try:
            sender_source = (
                self.offline_sender_combo.get() if self.offline_sender_combo else self.offline_user_combo.get()
            )
            sender_id = self._selected_id(sender_source)
            receiver_id = self._selected_id(self.offline_receiver_combo.get())
            amount = float(self.offline_tx_amount.get())
            self.platform.create_offline_transaction(sender_id, receiver_id, amount)
            self.refresh_all()
            messagebox.showinfo("Оффлайн-транзакция", "Оффлайн-транзакция создана и сохранена локально")
        except Exception as exc:
            messagebox.showerror("Ошибка", str(exc))

    def _ui_online_tx(self) -> None:
        try:
            sender_id = self._selected_id(self.sender_combo.get())
            receiver_id = self._selected_id(self.receiver_combo.get())
            amount = float(self.online_amount.get())
            channel = self.channel_combo.get()
            self.platform.create_online_transaction(sender_id, receiver_id, amount, channel)
            self.refresh_all()
            messagebox.showinfo("Онлайн транзакция", "Онлайн транзакция успешно выполнена и записана в реестр")
        except Exception as exc:
            messagebox.showerror("Ошибка", str(exc))

    def _ui_create_contract(self) -> None:
        try:
            sender_id = self._selected_id(self.contract_sender_combo.get())
            receiver_id = self._selected_id(self.contract_receiver_combo.get())
            bank_id = self._selected_id(self.contract_bank_combo.get())
            amount = float(self.contract_amount.get())
            description = self.contract_description.get()
            self.platform.create_smart_contract(
                sender_id, receiver_id, bank_id, amount, description
            )
            self.refresh_all()
            messagebox.showinfo("Смарт-контракт", "Контракт создан")
        except Exception as exc:
            messagebox.showerror("Ошибка", str(exc))

    def _ui_run_contracts(self) -> None:
        executed = self.platform.execute_due_contracts()
        self.refresh_all()
        messagebox.showinfo("Смарт-контракты", f"Исполнено контрактов: {len(executed)}")

    def _ui_request_emission(self) -> None:
        try:
            bank_id = self._selected_id(self.bank_combo.get())
            amount = float(self.emission_amount.get())
            req_id = self.platform.request_emission(bank_id, amount)
            self.refresh_all()
            messagebox.showinfo("Эмиссия", f"Запрос отправлен: {req_id}")
        except Exception as exc:
            messagebox.showerror("Ошибка", str(exc))

    def _ui_process_emission(self, approve: bool) -> None:
        selection = self.issuance_table.selection()
        if not selection:
            messagebox.showwarning("Эмиссия", "Выберите запрос")
            return
        item = self.issuance_table.item(selection[0])
        req_id = item["values"][0]
        reason = "" if approve else "Не выполнены условия резерва"
        try:
            self.platform.process_emission(req_id, approve, reason)
            self.refresh_all()
        except Exception as exc:
            messagebox.showerror("Ошибка", str(exc))

    def _ui_sync_offline(self) -> None:
        stats = self.platform.sync_offline_transactions()
        self.refresh_all()
        messagebox.showinfo(
            "Оффлайн синхронизация",
            f"Обработано: {stats['processed']}, Конфликты: {stats['conflicts']}",
        )

    def _ui_export_registry(self) -> None:
        folder = filedialog.askdirectory(title="Выберите папку для экспорта")
        if not folder:
            return
        paths = self.platform.export_registry(folder)
        messagebox.showinfo("Экспорт", f"Реестр экспортирован в файл:\n{paths['ledger']}")

    # endregion -------------------------------------------------------------------


if __name__ == "__main__":
    app = DigitalRubleApp()
    app.mainloop()

