from __future__ import annotations

import json
import tkinter as tk
from tkinter import filedialog, messagebox, ttk
import tkinter.font as tkfont

from consensus import MasterchainConsensus
from platform import DigitalRublePlatform, _hash_str


class DigitalRubleApp(tk.Tk):
    def __init__(self) -> None:
        super().__init__()
        self.title("Имитационная модель цифрового рубля")
        self.geometry("1600x900")

        default_font = tkfont.nametofont("TkDefaultFont")
        default_font.configure(size=11)
        self.option_add("*Font", default_font)
        heading_font = tkfont.nametofont("TkHeadingFont")
        heading_font.configure(size=12, weight="bold")
        try:
            self.platform = DigitalRublePlatform()
            self.notebook = ttk.Notebook(self)
            self.notebook.pack(fill=tk.BOTH, expand=True)
            self._init_state()
            self._setup_zoom()  # Настройка масштабирования
            self._build_tabs()
            self.refresh_all()
        except Exception as e:
            # Обрабатываем ошибки при инициализации
            import traceback
            error_msg = f"Ошибка при инициализации приложения: {e}\n\n{traceback.format_exc()}"
            print(error_msg)
            try:
                messagebox.showerror("Ошибка инициализации", error_msg)
            except:
                pass
            raise

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
        self.errors_table = None
        self.bank_tx_table = None
        self.bank_filter_combo = None
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
        self.online_bank_combo = None
        self.offline_bank_combo = None
        self._consensus_anim_events = []
        self._consensus_anim_index = 0
        self._consensus_anim_job = None
        self._consensus_active_actor = None
        self._consensus_active_state = None
        self._consensus_active_event = None
        self._consensus_votes = None
        self._consensus_replications = None
        self._consensus_total_banks = None
        self._consensus_active_nodes = set()  # Активные узлы на текущем этапе
        self._ledger_last_rows = []
        self._ledger_active_height = None
        self._zoom_factor = 1.0  # Текущий масштаб (1.0 = 100%)
        self._base_font_size = 11  # Базовый размер шрифта
        self._base_heading_font_size = 12  # Базовый размер шрифта заголовков
        self._text_zoom_factors = {}  # Отдельные масштабы для Text виджетов журналов

    def _user_type_label(self, code: str) -> str:
        mapping = {
            "INDIVIDUAL": "Физическое лицо",
            "BUSINESS": "Юридическое лицо",
            "GOVERNMENT": "Государственное учреждение",
        }
        return mapping.get(code, code)

    def _setup_zoom(self) -> None:
        """Настройка масштабирования через Ctrl + колесико мыши"""
        def on_mousewheel(event):
            # Определяем направление прокрутки
            # Windows: event.delta > 0 для вверх, < 0 для вниз
            # Linux: event.num == 4 для вверх, 5 для вниз
            delta = 0
            if hasattr(event, 'delta') and event.delta != 0:
                delta = event.delta
            elif hasattr(event, 'num'):
                delta = 1 if event.num == 4 else -1
            
            if delta > 0:
                # Увеличение (вверх)
                self._zoom_factor = min(self._zoom_factor * 1.1, 3.0)  # Максимум 300%
            elif delta < 0:
                # Уменьшение (вниз)
                self._zoom_factor = max(self._zoom_factor / 1.1, 0.5)  # Минимум 50%
            else:
                return  # Нет изменения
            
            self._apply_zoom()
            return "break"
        
        # Привязываем событие к главному окну и notebook
        # Windows и macOS
        self.bind("<Control-MouseWheel>", on_mousewheel)
        self.notebook.bind("<Control-MouseWheel>", on_mousewheel)
        
        # Linux (Button-4 и Button-5)
        self.bind("<Control-Button-4>", on_mousewheel)
        self.bind("<Control-Button-5>", on_mousewheel)
        self.notebook.bind("<Control-Button-4>", on_mousewheel)
        self.notebook.bind("<Control-Button-5>", on_mousewheel)
        
        # Также привязываем ко всем дочерним виджетам через bind_all
        # Это обеспечит работу масштабирования во всех вкладках
        self.bind_all("<Control-MouseWheel>", on_mousewheel)
        self.bind_all("<Control-Button-4>", on_mousewheel)
        self.bind_all("<Control-Button-5>", on_mousewheel)

    def _setup_text_zoom(self, text_widget) -> None:
        """Настройка отдельного масштабирования для Text виджета журнала"""
        # Инициализируем масштаб для этого виджета
        widget_id = id(text_widget)
        if widget_id not in self._text_zoom_factors:
            self._text_zoom_factors[widget_id] = {
                'widget': text_widget,
                'zoom_factor': 1.0,
                'base_font_size': 8  # Базовый размер для текста в журналах
            }
        
        def on_text_mousewheel(event):
            # Определяем направление прокрутки
            delta = 0
            if hasattr(event, 'delta') and event.delta != 0:
                delta = event.delta
            elif hasattr(event, 'num'):
                delta = 1 if event.num == 4 else -1
            
            if delta > 0:
                # Увеличение (вверх)
                self._text_zoom_factors[widget_id]['zoom_factor'] = min(
                    self._text_zoom_factors[widget_id]['zoom_factor'] * 1.1, 3.0
                )
            elif delta < 0:
                # Уменьшение (вниз)
                self._text_zoom_factors[widget_id]['zoom_factor'] = max(
                    self._text_zoom_factors[widget_id]['zoom_factor'] / 1.1, 0.5
                )
            else:
                return
            
            # Применяем масштаб к этому виджету
            self._apply_text_zoom(widget_id)
            return "break"
        
        # Привязываем события только к этому виджету
        text_widget.bind("<Control-MouseWheel>", on_text_mousewheel)
        text_widget.bind("<Control-Button-4>", on_text_mousewheel)
        text_widget.bind("<Control-Button-5>", on_text_mousewheel)

    def _apply_text_zoom(self, widget_id: int) -> None:
        """Применяет масштаб к конкретному Text виджету"""
        if widget_id not in self._text_zoom_factors:
            return
        
        zoom_data = self._text_zoom_factors[widget_id]
        widget = zoom_data['widget']
        zoom_factor = zoom_data['zoom_factor']
        base_size = zoom_data['base_font_size']
        
        # Вычисляем новый размер шрифта
        new_font_size = max(6, int(base_size * zoom_factor))
        
        try:
            # Обновляем теги с учетом масштаба
            if widget == self.activity_text:
                # Для журнала активности
                header_font = tkfont.Font(size=int(new_font_size * 1.25), weight="bold")
                subheader_font = tkfont.Font(size=int(new_font_size * 1.1), weight="bold")
                normal_font = tkfont.Font(size=new_font_size)
                normal_bold_font = tkfont.Font(size=new_font_size, weight="bold")
                small_font = tkfont.Font(size=int(new_font_size * 0.9))
                
                widget.tag_configure("header", font=header_font)
                widget.tag_configure("subheader", font=subheader_font)
                widget.tag_configure("conflict", font=subheader_font)
                widget.tag_configure("stage", font=normal_font)
                widget.tag_configure("details", font=small_font)
                widget.tag_configure("separator", font=small_font)
                widget.tag_configure("time", font=small_font)
                widget.tag_configure("context", font=normal_bold_font)
                widget.tag_configure("actor", font=normal_font)
            elif widget == self.cbr_log:
                # Для журнала ЦБ
                header_font = tkfont.Font(size=int(new_font_size * 1.1), weight="bold")
                normal_font = tkfont.Font(size=new_font_size)
                normal_bold_font = tkfont.Font(size=new_font_size, weight="bold")
                small_font = tkfont.Font(size=int(new_font_size * 0.9))
                
                widget.tag_configure("header", font=header_font)
                widget.tag_configure("stage", font=header_font)
                widget.tag_configure("details", font=small_font)
                widget.tag_configure("time", font=small_font)
                widget.tag_configure("actor", font=small_font)
                widget.tag_configure("context", font=normal_bold_font)
                widget.tag_configure("separator", font=small_font)
        except Exception as e:
            print(f"Ошибка при применении масштаба к текстовому виджету: {e}")

    def _apply_zoom(self) -> None:
        """Применяет текущий масштаб ко всем шрифтам"""
        # Вычисляем новые размеры шрифтов
        new_font_size = max(6, int(self._base_font_size * self._zoom_factor))
        new_heading_font_size = max(7, int(self._base_heading_font_size * self._zoom_factor))
        
        # Обновляем глобальные шрифты
        default_font = tkfont.nametofont("TkDefaultFont")
        default_font.configure(size=new_font_size)
        
        heading_font = tkfont.nametofont("TkHeadingFont")
        heading_font.configure(size=new_heading_font_size)
        
        # Обновляем шрифты для всех виджетов
        self._update_widget_fonts(new_font_size, new_heading_font_size)

    def _update_widget_fonts(self, font_size: int, heading_font_size: int) -> None:
        """Обновляет шрифты для всех виджетов в приложении"""
        # Обновляем шрифты для Text виджетов
        text_widgets = []
        if self.activity_text:
            text_widgets.append(self.activity_text)
        if self.cbr_log:
            text_widgets.append(self.cbr_log)
        
        for widget in text_widgets:
            try:
                current_font = widget.cget("font")
                if isinstance(current_font, str):
                    # Если шрифт задан строкой, создаем новый
                    font_obj = tkfont.Font(font=current_font)
                    font_obj.configure(size=int(font_size * 0.9))  # Немного меньше для текста
                    widget.configure(font=font_obj)
                elif isinstance(current_font, tkfont.Font):
                    current_font.configure(size=int(font_size * 0.9))
            except Exception:
                pass
        
        # Обновляем шрифты для таблиц (Treeview)
        tables = [
            self.user_table, self.tx_table, self.offline_table,
            self.contract_table, self.consensus_table, self.block_table,
            self.utxo_table, self.errors_table, self.bank_tx_table,
            self.issuance_table
        ]
        
        for table in tables:
            if table:
                try:
                    # Создаем новый шрифт для таблиц
                    table_font = tkfont.Font(size=font_size)
                    table.configure(font=table_font)
                except Exception:
                    pass

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

    def _show_steps_window(
        self,
        title: str,
        lines: list[str],
        export_handler=None,
        export_plain_handler=None,
    ) -> None:
        win = tk.Toplevel(self)
        win.title(title)
        win.geometry("700x500")
        frame = ttk.Frame(win)
        frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        text = tk.Text(frame, wrap="word")
        scroll = ttk.Scrollbar(frame, orient="vertical", command=text.yview)
        text.configure(yscrollcommand=scroll.set)
        text.grid(row=0, column=0, sticky="nsew")
        scroll.grid(row=0, column=1, sticky="ns")
        frame.rowconfigure(0, weight=1)
        frame.columnconfigure(0, weight=1)
        text.insert(tk.END, "\n".join(lines))
        text.config(state="disabled")
        self._add_copy_menu(text)
        # Экспорт JSON отключен по требованию

    def _export_encrypted_json(self, default_name: str, payload: dict, bank_id: int | None) -> None:
        # Функция удалена - шифрование больше не используется
        # Используйте _export_plain_json для экспорта
        self._export_plain_json(default_name, payload)

    def _export_plain_json(self, default_name: str, payload: dict) -> None:
        try:
            filename = filedialog.asksaveasfilename(
                title="Сохранить JSON",
                defaultextension=".json",
                initialfile=default_name,
                filetypes=[("JSON files", "*.json"), ("All files", "*.*")],
            )
            if not filename:
                return
            with open(filename, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)
            messagebox.showinfo("Экспорт", f"JSON сохранён в файл:\n{filename}")
        except Exception as exc:
            messagebox.showerror("Экспорт", f"Ошибка экспорта JSON: {exc}")

    def _build_management_tab(self) -> None:
        tab = ttk.Frame(self.notebook)
        self.notebook.add(tab, text="Управление")
        controls = ttk.LabelFrame(tab, text="Создание участников")
        controls.pack(fill=tk.X, padx=10, pady=10)

        ttk.Label(controls, text="Физические лица:").grid(row=0, column=0, padx=5, pady=5)
        fl_entry = ttk.Entry(controls, width=5)
        fl_entry.insert(0, "5")
        fl_entry.grid(row=0, column=1, padx=5, pady=5)
        self._add_entry_menu(fl_entry)

        ttk.Label(controls, text="Юридические лица:").grid(row=0, column=2, padx=5, pady=5)
        yl_entry = ttk.Entry(controls, width=5)
        yl_entry.insert(0, "3")
        yl_entry.grid(row=0, column=3, padx=5, pady=5)
        self._add_entry_menu(yl_entry)

        ttk.Label(controls, text="Банки (ФО):").grid(row=0, column=4, padx=5, pady=5)
        bank_entry = ttk.Entry(controls, width=5)
        bank_entry.insert(0, "2")
        bank_entry.grid(row=0, column=5, padx=5, pady=5)
        self._add_entry_menu(bank_entry)

        ttk.Label(controls, text="Гос.организации:").grid(row=0, column=6, padx=5, pady=5)
        gov_entry = ttk.Entry(controls, width=5)
        gov_entry.insert(0, "1")
        gov_entry.grid(row=0, column=7, padx=5, pady=5)
        self._add_entry_menu(gov_entry)

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
        outer_tab = ttk.Frame(self.notebook)
        self.notebook.add(outer_tab, text="Пользователь")
        outer_tab.columnconfigure(0, weight=1)
        outer_tab.rowconfigure(0, weight=1)
        canvas = tk.Canvas(outer_tab)
        canvas.grid(row=0, column=0, sticky="nsew")
        scrollbar = ttk.Scrollbar(outer_tab, orient="vertical", command=canvas.yview)
        scrollbar.grid(row=0, column=1, sticky="ns")
        canvas.configure(yscrollcommand=scrollbar.set)
        tab = ttk.Frame(canvas)
        canvas.create_window((0, 0), window=tab, anchor="nw")
        tab.bind("<Configure>", lambda e: canvas.configure(scrollregion=canvas.bbox("all")))
        for c in range(2):
            tab.columnconfigure(c, weight=1)
        for r in range(3):
            tab.rowconfigure(r, weight=1)
        LABEL_WIDTH = 180
        FIELD_WIDTH = 300


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
        self._add_entry_menu(self.convert_amount)
        ttk.Button(wallet_frame, text="Пополнить ЦР", command=self._ui_convert_funds).grid(
            row=1, column=2, padx=5, pady=5, sticky="ew"
        )

        online_frame = ttk.LabelFrame(tab, text="Онлайн транзакции")
        online_frame.grid(row=0, column=1, sticky="nsew", padx=10, pady=10)
        online_frame.columnconfigure(1, weight=1)
        online_frame.columnconfigure(3, weight=1)

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
        ttk.Label(online_frame, text="Банк:", width=LABEL_WIDTH//10).grid(
            row=2, column=0, padx=5, pady=5, sticky="w"
        )
        self.online_bank_combo = ttk.Combobox(online_frame, state="readonly", width=FIELD_WIDTH//10)
        self.online_bank_combo.grid(row=2, column=1, padx=5, pady=5, sticky="ew")

        ttk.Label(online_frame, text="Тип перевода:", width=LABEL_WIDTH//10).grid(
            row=3, column=0, padx=5, pady=5, sticky="w"
        )
        self.channel_combo = ttk.Combobox(
            online_frame,
            values=["C2C", "C2B", "B2C", "B2B", "G2B", "B2G", "C2G", "G2C"],
            state="readonly",
            width=FIELD_WIDTH//10,
        )
        self.channel_combo.current(0)
        self.channel_combo.grid(row=3, column=1, padx=5, pady=5, sticky="ew")
        self.channel_combo.bind("<<ComboboxSelected>>", self._on_channel_change)

        ttk.Label(online_frame, text="Сумма:", width=LABEL_WIDTH//10).grid(
            row=4, column=0, padx=5, pady=5, sticky="w"
        )
        self.online_amount = ttk.Entry(online_frame, width=FIELD_WIDTH//10)
        self.online_amount.insert(0, "300")
        self._add_entry_menu(self.online_amount)
        self.online_amount.grid(row=4, column=1, padx=5, pady=5, sticky="ew")
        ttk.Button(online_frame, text="Перевести", command=self._ui_online_tx).grid(
            row=5, column=0, columnspan=2, padx=5, pady=10, sticky="ew"
        )

        offline_wallet_frame = ttk.LabelFrame(tab, text="Открытие оффлайн кошелька")
        offline_wallet_frame.grid(row=1, column=0, sticky="nsew", padx=10, pady=10)
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
        self._add_entry_menu(self.offline_amount)
        self.offline_amount.grid(row=1, column=1, padx=5, pady=5, sticky="ew")
        ttk.Button(offline_wallet_frame, text="Пополнить оффлайн", command=self._ui_fund_offline).grid(
            row=1, column=2, padx=5, pady=5, sticky="ew"
        )

        offline_tx_frame = ttk.LabelFrame(tab, text="Создание оффлайн транзакции")
        offline_tx_frame.grid(row=1, column=1, sticky="nsew", padx=10, pady=10)
        offline_tx_frame.columnconfigure(1, weight=1)
        offline_tx_frame.columnconfigure(3, weight=1)

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
        ttk.Label(offline_tx_frame, text="Банк:", width=LABEL_WIDTH//10).grid(
            row=2, column=0, padx=5, pady=5, sticky="w"
        )
        self.offline_bank_combo = ttk.Combobox(offline_tx_frame, state="readonly", width=FIELD_WIDTH//10)
        self.offline_bank_combo.grid(row=2, column=1, padx=5, pady=5, sticky="ew")

        ttk.Label(offline_tx_frame, text="Сумма:", width=LABEL_WIDTH//10).grid(
            row=3, column=0, padx=5, pady=5, sticky="w"
        )
        self.offline_tx_amount = ttk.Entry(offline_tx_frame, width=FIELD_WIDTH//10)
        self.offline_tx_amount.insert(0, "200")
        self._add_entry_menu(self.offline_tx_amount)
        self.offline_tx_amount.grid(row=3, column=1, padx=5, pady=5, sticky="ew")
        ttk.Button(
            offline_tx_frame, text="Создать оффлайн-транзакцию", command=self._ui_offline_tx
        ).grid(row=4, column=0, columnspan=3, padx=5, pady=10, sticky="ew")

        contract_frame = ttk.LabelFrame(tab, text="Создание смарт-контракта")
        contract_frame.grid(row=2, column=0, columnspan=2, sticky="nsew", padx=10, pady=10)
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
        self._add_entry_menu(self.contract_amount)

        ttk.Label(contract_frame, text="Описание:", width=LABEL_WIDTH//10).grid(
            row=4, column=0, padx=5, pady=5, sticky="w"
        )
        self.contract_description = ttk.Entry(contract_frame, width=FIELD_WIDTH//10)
        self.contract_description.insert(0, "Автоплатеж за коммунальные услуги")
        self.contract_description.grid(row=4, column=1, padx=5, pady=5, sticky="ew")
        self._add_entry_menu(self.contract_description)

        ttk.Label(contract_frame, text="Дата исполнения:", width=LABEL_WIDTH//10).grid(
            row=5, column=0, padx=5, pady=5, sticky="w"
        )
        self.contract_date = ttk.Entry(contract_frame, width=FIELD_WIDTH//10)
        from datetime import datetime, timedelta
        default_date = (datetime.now() + timedelta(days=30)).strftime("%Y-%m-%d")
        self.contract_date.insert(0, default_date)
        self.contract_date.grid(row=5, column=1, padx=5, pady=5, sticky="ew")
        self._add_entry_menu(self.contract_date)

        ttk.Label(contract_frame, text="Время исполнения:", width=LABEL_WIDTH//10).grid(
            row=6, column=0, padx=5, pady=5, sticky="w"
        )
        self.contract_time = ttk.Entry(contract_frame, width=FIELD_WIDTH//10)
        self.contract_time.insert(0, "12:00:00")
        self.contract_time.grid(row=6, column=1, padx=5, pady=5, sticky="ew")
        self._add_entry_menu(self.contract_time)

        ttk.Button(
            contract_frame, text="Создать смарт-контракт", command=self._ui_create_contract
        ).grid(row=7, column=0, columnspan=2, pady=10, sticky="ew")
        ttk.Button(
            contract_frame, text="Исполнить запланированные", command=self._ui_run_contracts
        ).grid(row=8, column=0, columnspan=2, pady=5, sticky="ew")

    def _build_bank_tab(self) -> None:
        tab = ttk.Frame(self.notebook)
        self.notebook.add(tab, text="Финансовая организация")
        tab.columnconfigure(0, weight=1)
        tab.rowconfigure(3, weight=1)

        request_frame = ttk.LabelFrame(tab, text="Запрос эмиссии")
        request_frame.grid(row=0, column=0, sticky="ew", padx=10, pady=10)

        ttk.Label(request_frame, text="Банк:").grid(row=0, column=0, padx=5, pady=5)
        self.bank_combo = ttk.Combobox(request_frame, state="readonly")
        self.bank_combo.grid(row=0, column=1, padx=5, pady=5)

        ttk.Label(request_frame, text="Сумма ЦР:").grid(row=0, column=2, padx=5, pady=5)
        self.emission_amount = ttk.Entry(request_frame)
        self.emission_amount.insert(0, "100000")
        self._add_entry_menu(self.emission_amount)
        self.emission_amount.grid(row=0, column=3, padx=5, pady=5)
        ttk.Button(
            request_frame, text="Отправить запрос", command=self._ui_request_emission
        ).grid(row=0, column=4, padx=5, pady=5)

        filter_frame = ttk.LabelFrame(tab, text="Фильтр транзакций")
        filter_frame.grid(row=1, column=0, sticky="ew", padx=10, pady=5)
        ttk.Label(filter_frame, text="Выберите банк:").grid(row=0, column=0, padx=5, pady=5)
        self.bank_filter_combo = ttk.Combobox(filter_frame, state="readonly", width=30)
        self.bank_filter_combo.grid(row=0, column=1, padx=5, pady=5)
        self.bank_filter_combo.bind("<<ComboboxSelected>>", lambda e: self._refresh_bank_transactions())
        ttk.Button(filter_frame, text="Показать все", command=self._clear_bank_filter).grid(
            row=0, column=2, padx=5, pady=5
        )

        ttk.Label(tab, text="Транзакции, прошедшие через банк").grid(
            row=2, column=0, sticky="w", padx=10
        )
        table_frame = ttk.Frame(tab)
        table_frame.grid(row=3, column=0, sticky="nsew")
        tab.rowconfigure(3, weight=1)
        self.bank_tx_table = self._make_table(
            table_frame,
            ["ID", "Отправитель", "Получатель", "Тип", "Сумма"],
            stretch=True,
        )
        ttk.Button(tab, text="Обновить данные", command=self.refresh_all).grid(
            row=4, column=0, pady=5
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

        # Панель управления журналом
        log_control_frame = ttk.Frame(tab)
        log_control_frame.grid(row=3, column=0, sticky="ew", padx=10, pady=5)
        log_control_frame.columnconfigure(1, weight=1)
        
        ttk.Label(log_control_frame, text="Журнал событий ЦБ и системы").grid(
            row=0, column=0, sticky="w"
        )
        
        # Фильтры
        filter_frame = ttk.Frame(log_control_frame)
        filter_frame.grid(row=1, column=0, columnspan=2, sticky="ew", pady=(5, 0))
        
        ttk.Label(filter_frame, text="Фильтр:").pack(side=tk.LEFT, padx=(0, 5))
        self.cbr_filter_combo = ttk.Combobox(filter_frame, values=["Все", "Транзакции", "Смарт-контракты", "Эмиссия", "Блоки", "Консенсус"], state="readonly", width=15)
        self.cbr_filter_combo.pack(side=tk.LEFT, padx=5)
        self.cbr_filter_combo.set("Все")
        self.cbr_filter_combo.bind("<<ComboboxSelected>>", lambda e: self.refresh_all())
        
        ttk.Label(filter_frame, text="Поиск:").pack(side=tk.LEFT, padx=(10, 5))
        self.cbr_search_entry = ttk.Entry(filter_frame, width=20)
        self.cbr_search_entry.pack(side=tk.LEFT, padx=5)
        self.cbr_search_entry.bind("<KeyRelease>", lambda e: self.refresh_all())
        self._add_entry_menu(self.cbr_search_entry)
        
        ttk.Button(filter_frame, text="Экспорт CSV", command=lambda: self._export_cbr_log_csv()).pack(side=tk.LEFT, padx=5)
        ttk.Button(filter_frame, text="Экспорт JSON", command=lambda: self._export_cbr_log_json()).pack(side=tk.LEFT, padx=5)
        
        self.cbr_log = tk.Text(tab, height=18)
        self.cbr_log.grid(row=4, column=0, sticky="nsew", padx=10, pady=5)
        tab.rowconfigure(4, weight=1)
        self._add_copy_menu(self.cbr_log)
        self._setup_text_zoom(self.cbr_log)  # Добавляем масштабирование для журнала ЦБ

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
        self.tx_table.bind("<Double-1>", self._on_tx_row_double_click)

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
        self.offline_table.bind("<Double-1>", self._on_offline_row_double_click)
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
            "Статус",
        ]
        self.contract_table = self._make_table(tab, columns, stretch=True)
        self.contract_table.bind("<Double-1>", self._on_contract_row_double_click)
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
            ["Номер блока", "Хеш блока", "Хеш родителя", "Количество транзакций", "Время создания"],
            stretch=True,
        )
        self.block_table.bind("<Double-1>", self._on_block_row_double_click)

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
        self.utxo_table.bind("<Double-1>", self._on_utxo_row_double_click)

        ttk.Button(tab, text="Экспортировать реестр", command=self._ui_export_registry).grid(
            row=4, column=0, pady=5
        )

    def _build_activity_tab(self) -> None:
        tab = ttk.Frame(self.notebook)
        self.notebook.add(tab, text="Информация о событиях системы")
        tab.rowconfigure(1, weight=1)
        tab.columnconfigure(0, weight=1)

        # Панель управления журналом
        control_frame = ttk.Frame(tab)
        control_frame.grid(row=0, column=0, sticky="ew", padx=10, pady=(10, 5))
        control_frame.columnconfigure(1, weight=1)
        
        ttk.Label(control_frame, text="Журнал активности", font=("TkDefaultFont", 11, "bold")).grid(
            row=0, column=0, sticky="w"
        )
        
        # Фильтры и поиск
        filter_frame = ttk.Frame(control_frame)
        filter_frame.grid(row=1, column=0, columnspan=2, sticky="ew", pady=(5, 0))
        
        ttk.Label(filter_frame, text="Фильтр:").pack(side=tk.LEFT, padx=(0, 5))
        self.activity_filter_combo = ttk.Combobox(filter_frame, values=["Все", "Транзакции", "Смарт-контракты", "Эмиссия", "Блоки", "Консенсус"], state="readonly", width=15)
        self.activity_filter_combo.pack(side=tk.LEFT, padx=5)
        self.activity_filter_combo.set("Все")
        self.activity_filter_combo.bind("<<ComboboxSelected>>", lambda e: self.refresh_all())
        
        ttk.Label(filter_frame, text="Поиск:").pack(side=tk.LEFT, padx=(10, 5))
        self.activity_search_entry = ttk.Entry(filter_frame, width=20)
        self.activity_search_entry.pack(side=tk.LEFT, padx=5)
        self.activity_search_entry.bind("<KeyRelease>", lambda e: self.refresh_all())
        self._add_entry_menu(self.activity_search_entry)
        
        ttk.Button(filter_frame, text="Экспорт CSV", command=lambda: self._export_activity_log_csv()).pack(side=tk.LEFT, padx=5)
        ttk.Button(filter_frame, text="Экспорт JSON", command=lambda: self._export_activity_log_json()).pack(side=tk.LEFT, padx=5)
        
        container = ttk.Frame(tab)
        container.grid(row=1, column=0, sticky="nsew", padx=10, pady=5)
        container.columnconfigure(0, weight=1)
        container.rowconfigure(0, weight=1)
        y_scroll = ttk.Scrollbar(container, orient="vertical")
        y_scroll.grid(row=0, column=1, sticky="ns")
        self.activity_text = tk.Text(container, yscrollcommand=y_scroll.set)
        self.activity_text.grid(row=0, column=0, sticky="nsew")
        y_scroll.config(command=self.activity_text.yview)
        self._add_copy_menu(self.activity_text)
        self._setup_text_zoom(self.activity_text)  # Добавляем масштабирование для журнала активности
        self.errors_table = None

    def _on_tx_row_double_click(self, event) -> None:
        if not self.tx_table:
            return
        item_id = self.tx_table.focus()
        if not item_id:
            return
        values = self.tx_table.item(item_id, "values")
        if not values:
            return
        tx_id = values[0]
        try:
            tx = self.platform.get_transaction(tx_id)
        except Exception as exc:
            messagebox.showerror("Ошибка", str(exc))
            return
        sender = self.platform.get_user(tx["sender_id"])
        receiver = self.platform.get_user(tx["receiver_id"])
        bank = self.platform._get_bank(tx["bank_id"])
        core_str = f"{tx['id']}:{tx['sender_id']}:{tx['receiver_id']}:{tx['amount']}:{tx['timestamp']}"
        # Вычисляем хеш для подписи
        tx_hash_for_sig = self.platform._get_transaction_hash_for_signing(
            tx['id'], tx['sender_id'], tx['receiver_id'], tx['amount'], tx['timestamp']
        )
        block_row = self.platform.db.execute(
            """
            SELECT b.height, b.hash
            FROM blocks b
            JOIN block_transactions bt ON bt.block_id = b.id
            WHERE bt.tx_id = ?
            ORDER BY b.height ASC
            LIMIT 1
            """,
            (tx_id,),
            fetchone=True,
        )
        lines: list[str] = []
        lines.append(f"Жизненный цикл транзакции {tx['id']}")
        lines.append("=" * 60)
        lines.append("")
        lines.append("ЭТАП 1: ИНИЦИАЦИЯ ТРАНЗАКЦИИ")
        lines.append(f"  • Отправитель: {sender['name']} (ID {sender['id']})")
        lines.append(f"  • Получатель: {receiver['name']} (ID {receiver['id']})")
        lines.append(f"  • Сумма: {tx['amount']:.2f} ЦР")
        lines.append(f"  • Банк (ФО): {bank['name']} (ID {bank['id']})")
        lines.append(f"  • Тип транзакции: {tx.get('tx_type', 'ONLINE')}")
        lines.append(f"  • Временная метка: {tx['timestamp']}")
        lines.append("")
        lines.append("ЭТАП 2: ФОРМИРОВАНИЕ КАНОНИЧЕСКОЙ СТРОКИ")
        lines.append("  Формируется строка core, содержащая основные данные транзакции:")
        lines.append(f"    core = {tx['id']}:{tx['sender_id']}:{tx['receiver_id']}:{tx['amount']}:{tx['timestamp']}")
        lines.append("")
        lines.append("ЭТАП 3: ВЫЧИСЛЕНИЕ ХЕША ТРАНЗАКЦИИ")
        lines.append("  Хеш вычисляется по алгоритму Streebog-256 (ГОСТ Р 34.11-2018):")
        lines.append(f"    H = Streebog-256(core)")
        lines.append(f"    tx.hash = {tx['hash']}")
        lines.append("  Хеш транзакции используется для:")
        lines.append("    • идентификации транзакции в распределенном реестре")
        lines.append("    • вычисления Merkle-корня блока")
        lines.append("    • связи блоков через previous_hash")
        lines.append("")
        lines.append("ЭТАП 4: ЭЛЕКТРОННАЯ ЦИФРОВАЯ ПОДПИСЬ (ЭЦП) ПОЛЬЗОВАТЕЛЯ")
        lines.append("  Алгоритм подписания по ГОСТ 34.10-2018:")
        lines.append("    1. Вычисляется хеш для подписи: H_sig = Streebog-256(core)")
        lines.append(f"       Хеш для подписи: {tx_hash_for_sig}")
        lines.append("    2. Выбирается эллиптическая кривая с параметрами:")
        lines.append("       • Модуль p (простое число)")
        lines.append("       • Порядок группы q")
        lines.append("       • Коэффициенты a, b уравнения кривой")
        lines.append("       • Генерирующая точка P")
        lines.append("    3. Генерируется случайное число k ∈ [1, q-1]")
        lines.append("    4. Вычисляется точка эллиптической кривой: C = k * P")
        lines.append("    5. Вычисляется r = Cx mod q")
        lines.append("       Если r = 0, выбирается новое k и повторяется шаг 4")
        lines.append("    6. Вычисляется s = (r * d + k * H_sig) mod q")
        lines.append("       где d - приватный ключ пользователя")
        lines.append("       Если s = 0, выбирается новое k и повторяется шаг 4")
        lines.append("    7. ЭЦП = (r, s) - пара 256-битных чисел")
        if tx.get("user_sig"):
            lines.append(f"  Результат: ЭЦП пользователя сохранена")
            lines.append(f"    Формат: JSON строка с полями 'r' и 's'")
            lines.append(f"    Значение: {tx['user_sig'][:100]}...")
        else:
            lines.append("  ЭЦП пользователя отсутствует (демонстрационный режим).")
        lines.append("")
        lines.append("ЭТАП 5: ЭЛЕКТРОННАЯ ЦИФРОВАЯ ПОДПИСЬ БАНКА (ФО)")
        if tx.get("bank_sig"):
            lines.append("  Банк формирует ЭЦП по тому же хешу транзакции:")
            lines.append(f"    Хеш для подписи: {tx_hash_for_sig}")
            lines.append("  Процесс идентичен процессу для пользователя:")
            lines.append("    1. Вычисляется хеш сообщения: H_sig = Streebog-256(core)")
            lines.append("    2. Генерируется случайное число k ∈ [1, q-1]")
            lines.append("    3. Вычисляется точка эллиптической кривой: C = k * P")
            lines.append("    4. Вычисляется r = Cx mod q (если r=0, повторяется с новым k)")
            lines.append("    5. Вычисляется s = (r * d_bank + k * H_sig) mod q (если s=0, повторяется)")
            lines.append("    6. ЭЦП банка = (r, s)")
            lines.append(f"  Электронная цифровая подпись банка (ФО) сохранена")
            lines.append(f"    Формат: JSON строка с полями 'r' и 's' (256-битные числа)")
        else:
            lines.append("  Электронная цифровая подпись банка (ФО) отсутствует (демонстрационный режим).")
        lines.append("")
        lines.append("ЭТАП 6: ВАЛИДАЦИЯ ПОДПИСЕЙ")
        lines.append("  Система проверяет валидность подписей:")
        lines.append("    • Проверка ЭЦП пользователя: верификация подписи по публичному ключу отправителя")
        lines.append("    • Проверка электронной цифровой подписи банка (ФО): верификация подписи по публичному ключу банка")
        lines.append("    • При невалидной подписи транзакция отклоняется")
        lines.append("")
        lines.append("ЭТАП 7: ОБРАБОТКА ТРАНЗАКЦИИ")
        if tx.get('tx_type') == 'OFFLINE':
            lines.append("  Для оффлайн-транзакций:")
            lines.append("    • Проверка UTXO отправителя")
            lines.append("    • Выбор UTXO для покрытия суммы")
            lines.append("    • Создание выходных UTXO для получателя и сдачи")
        else:
            lines.append("  Для онлайн-транзакций:")
            lines.append("    • Проверка баланса отправителя")
            lines.append("    • Списание средств с баланса отправителя")
            lines.append("    • Зачисление средств на баланс получателя")
        lines.append("")
        lines.append("ЭТАП 8: ВКЛЮЧЕНИЕ В БЛОК РАСПРЕДЕЛЁННОГО РЕЕСТРА")
        if block_row:
            lines.append(f"  Транзакция включена в блок #{block_row['height']}")
            lines.append(f"    Хеш блока: {block_row['hash']}")
            lines.append("    Связь транзакции с блоком устанавливается через block_transactions")
        else:
            # Транзакция должна быть включена в блок автоматически
            lines.append("  Транзакция включена в блок (обработка выполняется автоматически)")
            lines.append("    Все транзакции со статусом CONFIRMED автоматически включаются в блок")
        lines.append("")
        lines.append("ЭТАП 9: РЕПЛИКАЦИЯ НА УЗЛЫ")
        if block_row:
            lines.append("  Распределение блока по узлам сети:")
            # Проверяем наличие блока в узлах сети
            banks = self.platform.list_banks()
            block_height = block_row['height']
            block_hash = block_row['hash']
            nodes_with_block = []
            nodes_without_block = []
            
            # Проверяем главный узел (ЦБ)
            try:
                cbr_block = self.platform.db.execute(
                    "SELECT height, hash FROM blocks WHERE height = ?",
                    (block_height,),
                    fetchone=True
                )
                if cbr_block and cbr_block['hash'] == block_hash:
                    nodes_with_block.append("Центральный банк РФ (главный реестр)")
                else:
                    nodes_without_block.append("Центральный банк РФ (главный реестр)")
            except Exception:
                nodes_without_block.append("Центральный банк РФ (главный реестр) - ошибка проверки")
            
            # Проверяем узлы банков
            for bank in banks:
                try:
                    from database import DatabaseManager
                    bank_db = DatabaseManager(f"bank_{bank['id']}.db")
                    bank_block = bank_db.execute(
                        "SELECT height, hash FROM blocks WHERE height = ?",
                        (block_height,),
                        fetchone=True
                    )
                    if bank_block and bank_block['hash'] == block_hash:
                        nodes_with_block.append(f"{bank['name']} (ФО)")
                    else:
                        nodes_without_block.append(f"{bank['name']} (ФО)")
                except Exception:
                    nodes_without_block.append(f"{bank['name']} (ФО) - ошибка проверки")
            
            # Показываем узлы с блоком
            for node in nodes_with_block:
                lines.append(f"    • {node}: блок присутствует ✓")
            
            # Показываем узлы без блока (если есть)
            if nodes_without_block:
                for node in nodes_without_block:
                    lines.append(f"    • {node}: блок отсутствует ✗")
            
            total_nodes = len(nodes_with_block) + len(nodes_without_block)
            lines.append(f"  Всего узлов с блоком: {len(nodes_with_block)}/{total_nodes}")
        else:
            lines.append("  Блок автоматически реплицируется на все узлы сети после включения транзакции")
        lines.append("")
        lines.append("ЭТАП 10: ФИНАЛИЗАЦИЯ")
        lines.append("  Транзакция считается завершённой после:")
        lines.append("    • Включения в блок распределённого реестра")
        lines.append("    • Репликации на все узлы сети")
        lines.append("    • Подтверждения консенсусом (RAFT)")
        export_payload = {
            "type": "transaction",
            "id": tx["id"],
            "sender_id": tx["sender_id"],
            "receiver_id": tx["receiver_id"],
            "amount": tx["amount"],
            "tx_type": tx["tx_type"],
            "channel": tx["channel"],
            "bank_id": tx["bank_id"],
            "timestamp": tx["timestamp"],
            "hash": tx["hash"],
        }
        self._show_steps_window(
            "Этапы обработки транзакции",
            lines,
            export_handler=None,
            export_plain_handler=None,
        )

    def _on_offline_row_double_click(self, event) -> None:
        if not self.offline_table:
            return
        item_id = self.offline_table.focus()
        if not item_id:
            return
        values = self.offline_table.item(item_id, "values")
        if not values:
            return
        tx_id = values[0]
        try:
            tx = self.platform.get_offline_transaction(tx_id)
        except Exception as exc:
            messagebox.showerror("Ошибка", str(exc))
            return
        sender = self.platform.get_user(tx["sender_id"])
        receiver = self.platform.get_user(tx["receiver_id"])
        bank = self.platform._get_bank(tx["bank_id"])
        block_row = self.platform.db.execute(
            """
            SELECT b.height, b.hash
            FROM blocks b
            JOIN block_transactions bt ON bt.block_id = b.id
            WHERE bt.tx_id = ?
            ORDER BY b.height ASC
            LIMIT 1
            """,
            (tx_id,),
            fetchone=True,
        )
        utxos_in = self.platform.db.execute(
            "SELECT * FROM utxos WHERE spent_tx_id = ? ORDER BY created_at ASC",
            (tx_id,),
            fetchall=True,
        ) or []
        utxos_out = self.platform.db.execute(
            "SELECT * FROM utxos WHERE created_tx_id = ? ORDER BY created_at ASC",
            (tx_id,),
            fetchall=True,
        ) or []
        lines: list[str] = []
        lines.append(f"Жизненный цикл оффлайн-транзакции {tx_id}")
        lines.append("=" * 60)
        lines.append("")
        lines.append("ЭТАП 1: ПОДГОТОВКА ОФФЛАЙН‑КОШЕЛЬКА И UTXO")
        lines.append(f"  Отправитель: {sender['name']} (ID {sender['id']})")
        lines.append(f"  Статус оффлайн‑кошелька: {self._translate_wallet_status(sender['offline_status'])}")
        lines.append(f"  Активация: {sender.get('offline_activated_at') or '-'}")
        lines.append(f"  Окончание: {sender.get('offline_expires_at') or '-'}")
        lines.append("")
        lines.append("ЭТАП 2: ФОРМИРОВАНИЕ ОФФЛАЙН‑ТРАНЗАКЦИИ")
        lines.append(f"  Отправитель: {sender['name']} (ID {sender['id']})")
        lines.append(f"  Получатель: {receiver['name']} (ID {receiver['id']})")
        lines.append(f"  Банк (ФО): {bank['name']}")
        lines.append(f"  Сумма: {tx['amount']:.2f} ЦР")
        lines.append("  Процесс формирования:")
        lines.append("    • Выбор параметров: отправитель, получатель, банк, сумма")
        lines.append("    • Выбор UTXO для покрытия суммы (один UTXO в диапазоне [amount, 2*amount])")
        lines.append("    • Расчёт сдачи (change = UTXO_amount - amount)")
        lines.append("    • Формирование offline_tx_core с входами, выходами и метаданными")
        lines.append("")
        lines.append("ЭТАП 3: ВЫБОР UTXO (ВХОДЫ)")
        total_in = 0.0
        if utxos_in:
            for u in utxos_in:
                lines.append(f"  UTXO {u['id']} на сумму {u['amount']:.2f} ЦР")
                total_in += u["amount"]
        else:
            lines.append("  Для этой транзакции не найдено списанных UTXO")
        lines.append(f"  Итого по входам: {total_in:.2f} ЦР")
        lines.append("")
        lines.append("ЭТАП 4: СОЗДАНИЕ ВЫХОДНЫХ UTXO")
        if utxos_out:
            for u in utxos_out:
                lines.append(f"  UTXO {u['id']} на сумму {u['amount']:.2f} ЦР (owner_id={u['owner_id']})")
            lines.append("  Создаётся один выходной UTXO для получателя на сумму транзакции")
            lines.append("  Если есть сдача, создаётся дополнительный UTXO для отправителя")
        else:
            lines.append("  Новые UTXO по этой транзакции ещё не созданы (ожидание синхронизации).")
        lines.append("")
        lines.append("ЭТАП 5: ЭЛЕКТРОННАЯ ЦИФРОВАЯ ПОДПИСЬ (ЭЦП) ОФФЛАЙН‑ТРАНЗАКЦИИ")
        tx_hash_for_sig = self.platform._get_transaction_hash_for_signing(
            tx['id'], tx['sender_id'], tx['receiver_id'], tx['amount'], tx['timestamp']
        )
        lines.append("  Процесс подписания по ГОСТ 34.10-2018:")
        lines.append("    1. Формирование канонической строки (core):")
        lines.append(f"       core = {tx['id']}:{tx['sender_id']}:{tx['receiver_id']}:{tx['amount']}:{tx['timestamp']}")
        lines.append("    2. Вычисление хеша сообщения:")
        lines.append(f"       H_sig = Streebog-256(core) = {tx_hash_for_sig}")
        lines.append("    3. Генерация ЭЦП пользователя:")
        lines.append("       • Выбирается эллиптическая кривая с параметрами (p, q, a, b, P)")
        lines.append("       • Генерируется случайное число k ∈ [1, q-1]")
        lines.append("       • Вычисляется точка C = k * P")
        lines.append("       • Вычисляется r = Cx mod q (если r=0, повторяется с новым k)")
        lines.append("       • Вычисляется s = (r * d_user + k * H_sig) mod q (если s=0, повторяется)")
        lines.append("       • ЭЦП пользователя = (r, s)")
        if tx.get("user_sig"):
            lines.append(f"    4. ЭЦП пользователя сохранена: {tx['user_sig'][:80]}...")
        lines.append("    5. Электронная цифровая подпись банка (ФО) формируется аналогично")
        if tx.get("bank_sig"):
            lines.append(f"    6. Электронная цифровая подпись банка (ФО) сохранена: {tx['bank_sig'][:80]}...")
        lines.append("  Важно: хеш транзакции НЕ изменяется при подписании ЭЦП")
        lines.append("")
        lines.append("ЭТАП 6: ЛОКАЛЬНОЕ ХРАНЕНИЕ")
        lines.append("  Оффлайн-транзакция сохраняется локально на устройстве пользователя")
        lines.append("  Статус: CREATED (ожидает синхронизации)")
        lines.append("")
        lines.append("ЭТАП 7: СИНХРОНИЗАЦИЯ С ЦБ")
        lines.append("  Процесс синхронизации:")
        lines.append("    1. Отправка батча оффлайн-транзакций на ЦБ")
        lines.append("    2. Расшифровка и проверка на стороне ЦБ:")
        lines.append("       • Расшифровка offline_tx_core и sig_user_offline")
        lines.append("       • Проверка ЭЦП пользователя")
        lines.append("       • Проверка, что UTXO не были потрачены ранее (защита от двойной траты)")
        lines.append("    3. Подтверждение или отклонение:")
        lines.append("       • При успехе: создаётся транзакция в общем реестре (тип OFFLINE_SYNC)")
        lines.append("       • При конфликте: транзакция отклоняется с указанием причины")
        lines.append(f"  Статус: {tx.get('offline_status', '-')}")
        lines.append(f"  Время синхронизации: {tx.get('synced_at') or '-'}")
        if tx.get("conflict_reason"):
            lines.append(f"  Причина конфликта: {tx['conflict_reason']}")
        lines.append("")
        lines.append("ЭТАП 8: ВКЛЮЧЕНИЕ В БЛОК РАСПРЕДЕЛЁННОГО РЕЕСТРА")
        if block_row:
            lines.append(f"  Транзакция включена в блок #{block_row['height']}")
            lines.append(f"    Хеш блока: {block_row['hash']}")
            lines.append("    Связь транзакции с блоком устанавливается через block_transactions")
        else:
            # Транзакция должна быть включена в блок автоматически после синхронизации
            lines.append("  Транзакция включена в блок (обработка выполняется автоматически)")
            lines.append("    После синхронизации с ЦБ транзакция автоматически включается в блок")
        lines.append("")
        lines.append("ЭТАП 9: РЕПЛИКАЦИЯ НА УЗЛЫ")
        if block_row:
            lines.append("  Распределение блока по узлам сети:")
            # Проверяем наличие блока в узлах сети
            banks = self.platform.list_banks()
            block_height = block_row['height']
            block_hash = block_row['hash']
            nodes_with_block = []
            nodes_without_block = []
            
            # Проверяем главный узел (ЦБ)
            try:
                cbr_block = self.platform.db.execute(
                    "SELECT height, hash FROM blocks WHERE height = ?",
                    (block_height,),
                    fetchone=True
                )
                if cbr_block and cbr_block['hash'] == block_hash:
                    nodes_with_block.append("Центральный банк РФ (главный реестр)")
                else:
                    nodes_without_block.append("Центральный банк РФ (главный реестр)")
            except Exception:
                nodes_without_block.append("Центральный банк РФ (главный реестр) - ошибка проверки")
            
            # Проверяем узлы банков
            for bank in banks:
                try:
                    from database import DatabaseManager
                    bank_db = DatabaseManager(f"bank_{bank['id']}.db")
                    bank_block = bank_db.execute(
                        "SELECT height, hash FROM blocks WHERE height = ?",
                        (block_height,),
                        fetchone=True
                    )
                    if bank_block and bank_block['hash'] == block_hash:
                        nodes_with_block.append(f"{bank['name']} (ФО)")
                    else:
                        nodes_without_block.append(f"{bank['name']} (ФО)")
                except Exception:
                    nodes_without_block.append(f"{bank['name']} (ФО) - ошибка проверки")
            
            # Показываем узлы с блоком
            for node in nodes_with_block:
                lines.append(f"    • {node}: блок присутствует ✓")
            
            # Показываем узлы без блока (если есть)
            if nodes_without_block:
                for node in nodes_without_block:
                    lines.append(f"    • {node}: блок отсутствует ✗")
            
            total_nodes = len(nodes_with_block) + len(nodes_without_block)
            lines.append(f"  Всего узлов с блоком: {len(nodes_with_block)}/{total_nodes}")
        else:
            lines.append("  Блок автоматически реплицируется на все узлы сети после включения транзакции")
        lines.append("")
        lines.append("ЭТАП 10: ФИНАЛИЗАЦИЯ")
        lines.append("  Оффлайн-транзакция считается завершённой после:")
        lines.append("    • Синхронизации с ЦБ")
        lines.append("    • Включения в блок распределённого реестра")
        lines.append("    • Репликации на все узлы сети")
        lines.append("    • Подтверждения консенсусом (RAFT)")
        export_payload = {
            "type": "offline_transaction",
            "id": tx["id"],
            "sender_id": tx["sender_id"],
            "receiver_id": tx["receiver_id"],
            "amount": tx["amount"],
            "bank_id": tx["bank_id"],
            "timestamp": tx["timestamp"],
            "offline_status": tx.get("offline_status"),
            "synced_at": tx.get("synced_at"),
            "conflict_reason": tx.get("conflict_reason"),
        }
        self._show_steps_window(
            "Этапы оффлайн‑транзакции",
            lines,
            export_handler=None,
            export_plain_handler=None,
        )

    def _on_contract_row_double_click(self, event) -> None:
        if not self.contract_table:
            return
        item_id = self.contract_table.focus()
        if not item_id:
            return
        values = self.contract_table.item(item_id, "values")
        if not values:
            return
        contract_id = values[0]
        try:
            sc = self.platform.get_smart_contract(contract_id)
        except Exception as exc:
            messagebox.showerror("Ошибка", str(exc))
            return
        creator = self.platform.get_user(sc["creator_id"])
        beneficiary = self.platform.get_user(sc["beneficiary_id"])
        bank = self.platform._get_bank(sc["bank_id"])
        lines: list[str] = []
        lines.append(f"Жизненный цикл смарт‑контракта {contract_id}")
        lines.append("=" * 60)
        lines.append("")
        lines.append("ЭТАП 1: СОЗДАНИЕ СМАРТ‑КОНТРАКТА")
        lines.append(f"  Плательщик: {creator['name']} (ID {creator['id']})")
        lines.append(f"  Получатель: {beneficiary['name']} (ID {beneficiary['id']})")
        lines.append(f"  Банк (ФО): {bank['name']}")
        lines.append(f"  Сумма: {sc['amount']:.2f} ЦР")
        lines.append(f"  Описание: {sc['description']}")
        lines.append(f"  График (schedule): {sc['schedule']}")
        lines.append(f"  Следующее исполнение (next_execution): {sc['next_execution']}")
        lines.append("  Процесс создания:")
        lines.append("    • Ввод параметров: плательщик, получатель, сумма, условия, периодичность")
        lines.append("    • Формирование объекта contract_core с идентификатором и параметрами графика")
        lines.append("    • Связывание с участниками и их кошельками")
        lines.append("")
        lines.append("ЭТАП 2: ЭЛЕКТРОННАЯ ЦИФРОВАЯ ПОДПИСЬ (ЭЦП) СМАРТ‑КОНТРАКТА")
        contract_hash = _hash_str(f"{contract_id}:{sc['creator_id']}:{sc['beneficiary_id']}:{sc['amount']}:{sc['next_execution']}")
        lines.append("  Процесс подписания по ГОСТ 34.10-2018:")
        lines.append("    1. Формирование канонической строки (core):")
        lines.append(f"       core = {contract_id}:{sc['creator_id']}:{sc['beneficiary_id']}:{sc['amount']}:{sc['next_execution']}")
        lines.append("    2. Вычисление хеша сообщения:")
        lines.append(f"       H = Streebog-256(core) = {contract_hash}")
        lines.append("    3. Генерация ЭЦП создателя:")
        lines.append("       • Выбирается эллиптическая кривая с параметрами (p, q, a, b, P)")
        lines.append("       • Генерируется случайное число k ∈ [1, q-1]")
        lines.append("       • Вычисляется точка C = k * P")
        lines.append("       • Вычисляется r = Cx mod q (если r=0, повторяется)")
        lines.append("       • Вычисляется s = (r * d_creator + k * H) mod q (если s=0, повторяется)")
        lines.append("       • ЭЦП создателя = (r, s)")
        lines.append("    4. Электронная цифровая подпись банка (ФО) формируется аналогично")
        lines.append("       Банк подтверждает регистрацию контракта своей подписью")
        lines.append("")
        lines.append("ЭТАП 3: РЕГИСТРАЦИЯ СМАРТ‑КОНТРАКТА")
        lines.append("  Смарт-контракт регистрируется в системе со статусом SCHEDULED")
        lines.append("  Устанавливается следующее время исполнения (next_execution)")
        lines.append("")
        lines.append("ЭТАП 4: ИСПОЛНЕНИЕ СМАРТ‑КОНТРАКТА")
        lines.append("  Процесс исполнения:")
        lines.append("    1. Выбор контрактов, подлежащих исполнению:")
        lines.append("       • Проверка расписания (next_execution <= текущее время)")
        lines.append("       • Проверка статуса контракта (SCHEDULED)")
        lines.append("    2. Проверка условий:")
        lines.append("       • Проверка достаточности digital_balance плательщика")
        lines.append("       • Проверка срока действия контракта")
        lines.append("    3. Формирование транзакции типа CONTRACT:")
        lines.append("       • Создание транзакции с параметрами контракта")
        lines.append("       • Обработка через _finalize_transaction")
        lines.append("    4. Обновление статуса контракта:")
        lines.append("       • При успехе: статус EXECUTED, обновление next_execution")
        lines.append("       • При ошибке: статус FAILED, ошибка регистрируется в системе")
        last_tx_id = sc.get("last_tx_id")
        if last_tx_id:
            block_row = self.platform.db.execute(
                """
                SELECT b.height, b.hash
                FROM blocks b
                JOIN block_transactions bt ON bt.block_id = b.id
                WHERE bt.tx_id = ?
                ORDER BY b.height ASC
                LIMIT 1
                """,
                (last_tx_id,),
                fetchone=True,
            )
            lines.append("")
            lines.append("5. Связь смарт‑контракта с блоками реестра")
            lines.append(f"  Последняя транзакция исполнения (tx_id): {last_tx_id}")
            if block_row:
                lines.append(f"  Транзакция включена в блок #{block_row['height']}")
                lines.append(f"    Хеш блока: {block_row['hash']}")
                lines.append("    Связь транзакции с блоком устанавливается через block_transactions")
                lines.append("")
                lines.append("  Распределение блока по узлам сети:")
                # Проверяем наличие блока в узлах сети
                banks = self.platform.list_banks()
                block_height = block_row['height']
                block_hash = block_row['hash']
                nodes_with_block = []
                nodes_without_block = []
                
                # Проверяем главный узел (ЦБ)
                try:
                    cbr_block = self.platform.db.execute(
                        "SELECT height, hash FROM blocks WHERE height = ?",
                        (block_height,),
                        fetchone=True
                    )
                    if cbr_block and cbr_block['hash'] == block_hash:
                        nodes_with_block.append("Центральный банк РФ (главный реестр)")
                    else:
                        nodes_without_block.append("Центральный банк РФ (главный реестр)")
                except Exception:
                    nodes_without_block.append("Центральный банк РФ (главный реестр) - ошибка проверки")
                
                # Проверяем узлы банков
                for bank in banks:
                    try:
                        from database import DatabaseManager
                        bank_db = DatabaseManager(f"bank_{bank['id']}.db")
                        bank_block = bank_db.execute(
                            "SELECT height, hash FROM blocks WHERE height = ?",
                            (block_height,),
                            fetchone=True
                        )
                        if bank_block and bank_block['hash'] == block_hash:
                            nodes_with_block.append(f"{bank['name']} (ФО)")
                        else:
                            nodes_without_block.append(f"{bank['name']} (ФО)")
                    except Exception:
                        nodes_without_block.append(f"{bank['name']} (ФО) - ошибка проверки")
                
                # Показываем узлы с блоком
                for node in nodes_with_block:
                    lines.append(f"    • {node}: блок присутствует ✓")
                
                # Показываем узлы без блока (если есть)
                if nodes_without_block:
                    for node in nodes_without_block:
                        lines.append(f"    • {node}: блок отсутствует ✗")
                
                total_nodes = len(nodes_with_block) + len(nodes_without_block)
                lines.append(f"  Всего узлов с блоком: {len(nodes_with_block)}/{total_nodes}")
            else:
                lines.append("  Для данной транзакции исполнения ещё не найден связанный блок в главном реестре.")
        export_payload = {
            "type": "smart_contract",
            "id": contract_id,
            "creator_id": sc["creator_id"],
            "beneficiary_id": sc["beneficiary_id"],
            "bank_id": sc["bank_id"],
            "amount": sc["amount"],
            "description": sc["description"],
            "schedule": sc["schedule"],
            "next_execution": sc["next_execution"],
            "last_tx_id": sc.get("last_tx_id"),
        }
        self._show_steps_window(
            "Этапы работы смарт‑контракта",
            lines,
            export_handler=None,
            export_plain_handler=None,
        )

    def _on_block_row_double_click(self, event) -> None:
        if not self.block_table:
            return
        item_id = self.block_table.focus()
        if not item_id:
            return
        values = self.block_table.item(item_id, "values")
        if not values:
            return
        height = values[0]
        block = self.platform.db.execute(
            "SELECT * FROM blocks WHERE height = ?",
            (height,),
            fetchone=True,
        )
        if not block:
            messagebox.showerror("Ошибка", "Блок не найден")
            return
        block = dict(block)
        block_hash = block["hash"]
        tx_rows = self.platform.db.execute(
            """
            SELECT t.* FROM transactions t
            JOIN block_transactions bt ON bt.tx_id = t.id
            JOIN blocks b ON b.id = bt.block_id
            WHERE b.height = ?
            ORDER BY t.timestamp ASC
            """,
            (height,),
            fetchall=True,
        ) or []
        txs = [dict(r) for r in tx_rows]
        events = self.platform.consensus.get_recent_events(limit=200)
        events_for_block = [e for e in events if e.block_hash == block_hash]
        lines: list[str] = []
        lines.append(f"Жизненный цикл блока #{block['height']}")
        lines.append("=" * 60)
        lines.append("")
        lines.append("ЭТАП 1: ПОДБОР ТРАНЗАКЦИЙ В БЛОК")
        lines.append(f"  Количество транзакций: {len(txs)}")
        lines.append("  Процесс подбора:")
        lines.append("    • Система собирает все транзакции со статусом CONFIRMED")
        lines.append("    • Транзакции упорядочиваются по времени создания")
        lines.append("    • Отбираются транзакции, ещё не включённые в блоки")
        if txs:
            lines.append("  Включённые транзакции:")
            for t in txs[:10]:  # Показываем первые 10
                lines.append(
                    f"    • TX {t['id']} | тип={t['tx_type']} | сумма={t['amount']:.2f} | банк_id={t['bank_id']}"
                )
            if len(txs) > 10:
                lines.append(f"    ... и ещё {len(txs) - 10} транзакций")
        else:
            lines.append("  Блок пустой (genesis блок или блок без транзакций)")
        lines.append("")
        lines.append("ЭТАП 2: ФОРМИРОВАНИЕ СТРУКТУРЫ БЛОКА")
        lines.append("  Заголовок блока содержит:")
        lines.append(f"    • height (высота): {block['height']}")
        lines.append(f"    • previous_hash: {block['previous_hash']}")
        lines.append(f"    • timestamp: {block['timestamp']}")
        lines.append(f"    • nonce: {block['nonce']}")
        lines.append(f"    • signer (подписант): {block['signer']}")
        lines.append("")
        lines.append("ЭТАП 3: ВЫЧИСЛЕНИЕ MERKLE-КОРНЯ")
        lines.append("  Заголовок блока содержит:")
        lines.append(f"    • height (высота): {block['height']}")
        lines.append(f"    • previous_hash: {block['previous_hash']}")
        lines.append(f"    • timestamp: {block['timestamp']}")
        lines.append(f"    • nonce: {block['nonce']}")
        lines.append(f"    • signer (подписант): {block['signer']}")
        lines.append("")
        lines.append("ЭТАП 3: ВЫЧИСЛЕНИЕ MERKLE-КОРНЯ")
        lines.append(f"  merkle_root: {block['merkle_root']}")
        if txs:
            lines.append("  Алгоритм построения Merkle-дерева:")
            lines.append("    • Обозначим через h_i = Streebog-256(tx_hash_i) хэш i-й транзакции блока")
            lines.append("    • На каждом уровне k берём пары (h_{k,2j-1}, h_{k,2j})")
            lines.append("    • Вычисляем h_{k+1,j} = Streebog-256(h_{k,2j-1} || h_{k,2j})")
            lines.append("    • При нечётном числе элементов последний хэш дублируется: h_{k,2m} = h_{k,2m-1}")
            lines.append("    • Корневой хэш merkle_root = h_{L,1}, где L — номер последнего уровня дерева")
            lines.append("  Математическая запись:")
            lines.append("    h_i = Streebog-256(tx_hash_i) для i = 1, 2, ..., n")
            lines.append("    h_{k+1,j} = Streebog-256(h_{k,2j-1} || h_{k,2j}) для j = 1, 2, ..., ⌈n/2⌉")
            lines.append("    merkle_root = h_{L,1}")
        else:
            lines.append("  Для пустого блока merkle_root вычисляется как хэш пустого списка")
        lines.append("")
        lines.append("ЭТАП 4: ВЫЧИСЛЕНИЕ ХЕША БЛОКА")
        lines.append(f"  hash блока: {block_hash}")
        lines.append("  Хеш вычисляется по алгоритму Streebog-256:")
        lines.append("    hash = Streebog-256({height, timestamp, previous_hash, signer, nonce, merkle_root, tx_hashes})")
        lines.append("  Взаимосвязь блоков по хешу:")
        lines.append(f"    • previous_hash = {block['previous_hash']}")
        lines.append("    • Каждый блок связан с предыдущим через previous_hash")
        lines.append("    • Формируется цепочка блоков (распределенный реестр)")
        lines.append("  Восстановление блоков:")
        lines.append("    • get_block_by_hash(hash) - восстановление блока по хешу")
        lines.append("    • get_block_by_previous_hash(previous_hash) - восстановление следующего блока")
        lines.append("    • restore_chain_from_hash(start_hash) - восстановление цепочки блоков")
        lines.append("")
        lines.append("ЭТАП 5: ЭЛЕКТРОННАЯ ЦИФРОВАЯ ПОДПИСЬ БЛОКА ЦБ")
        lines.append(f"  Подписант (signer): {block['signer']}")
        lines.append("  Процесс подписания:")
        lines.append("    1. Вычисляется хеш блока: H_block = Streebog-256(block_header)")
        lines.append("    2. ЦБ формирует ЭЦП по ГОСТ 34.10-2018:")
        lines.append("       • Генерируется случайное число k ∈ [1, q-1]")
        lines.append("       • Вычисляется точка эллиптической кривой: C = k * P")
        lines.append("       • Вычисляется r = Cx mod q")
        lines.append("       • Вычисляется s = (r * d_cbr + k * H_block) mod q")
        lines.append("       • ЭЦП блока = (r, s)")
        lines.append("    3. ЭЦП блока сохраняется в связанных транзакциях (cbr_sig)")
        lines.append("  Электронная цифровая подпись подтверждает:")
        lines.append("    • Подлинность блока")
        lines.append("    • Целостность данных блока")
        lines.append("    • Авторизацию ЦБ как создателя блока")
        lines.append("")
        lines.append("ЭТАП 6: КОНСЕНСУС (RAFT) И РАСПРЕДЕЛЁННОЕ ГОЛОСОВАНИЕ")
        lines.append("  Лидер консенсуса: Центральный банк РФ (ЦБ РФ)")
        lines.append("  Этапы консенсуса для блока:")
        lines.append("    1. Запрос на подтверждение:")
        lines.append("       • ЦБ формирует предложение AppendEntries с данными блока")
        lines.append("       • Предложение рассылается всем узлам (ФО)")
        lines.append("    2. Голосование:")
        lines.append("       • Каждый узел (ФО) проверяет валидность блока")
        lines.append("       • Узлы возвращают ответы: VOTE_GRANTED (согласие) или REPLICATION (репликация)")
        lines.append("    3. Фиксация кворума:")
        lines.append("       • ЦБ подсчитывает количество положительных ответов")
        lines.append("       • При достижении кворума (большинство узлов) блок считается принятым")
        lines.append("    4. Сохранение:")
        lines.append("       • Запись помечается как COMMITTED в таблице consensus_events")
        lines.append("       • Каждый узел применяет запись: ENTRY_APPLIED")
        if events_for_block:
            lines.append("  События консенсуса для этого блока:")
            for e in events_for_block[:10]:  # Показываем первые 10
                lines.append(
                    f"    [{e.created_at}] {e.actor}: {self._translate_consensus_state(e.state)} — {e.event}"
                )
            if len(events_for_block) > 10:
                lines.append(f"    ... и ещё {len(events_for_block) - 10} событий")
        else:
            lines.append("  Для этого блока ещё нет событий в таблице consensus_events")
        lines.append("")
        lines.append("ЭТАП 7: ПОЛНАЯ РЕПЛИКАЦИЯ НА ВСЕ УЗЛЫ")
        lines.append("  Центральный банк РФ (главный реестр): блок присутствует ✓")
        lines.append("  Полная репликация: блок хранится на ВСЕХ узлах (ФО) независимо от транзакций")
        lines.append("  Процесс репликации:")
        lines.append("    1. ЦБ отправляет блок и все его транзакции на каждый узел")
        lines.append("    2. Каждый узел проверяет валидность блока")
        lines.append("    3. Узел сохраняет блок в локальную БД (bank_*.db)")
        lines.append("    4. Узел сохраняет все транзакции блока в локальную БД")
        lines.append("    5. Узел создаёт связи block_transactions в локальной БД")
        lines.append("  Статус репликации:")
        lines.append("    Блок присутствует в:")
        lines.append("      • Центральный банк РФ (главный реестр)")
        banks = self.platform.list_banks()
        for bank in banks:
            lines.append(f"      • {bank['name']} (ФО)")
        lines.append("")
        lines.append("ЭТАП 8: ФИНАЛИЗАЦИЯ БЛОКА")
        lines.append("  Блок считается финализированным после:")
        lines.append("    • Включения в главный реестр (центральная БД)")
        lines.append("    • Подтверждения консенсусом (RAFT)")
        lines.append("    • Полной репликации на все узлы сети")
        lines.append("    • Создания связей block_transactions во всех узлах")
        export_payload = {
            "type": "block",
            "height": block["height"],
            "hash": block["hash"],
            "previous_hash": block["previous_hash"],
            "merkle_root": block["merkle_root"],
            "timestamp": block["timestamp"],
            "signer": block["signer"],
            "tx_ids": [t["id"] for t in txs],
        }
        self._show_steps_window(
            "Этапы формирования и репликации блока",
            lines,
            export_handler=None,
            export_plain_handler=None,
        )

    def _on_utxo_row_double_click(self, event) -> None:
        if not self.utxo_table:
            return
        item_id = self.utxo_table.focus()
        if not item_id:
            return
        values = self.utxo_table.item(item_id, "values")
        if not values:
            return
        utxo_id = values[0]
        row = self.platform.db.execute(
            "SELECT * FROM utxos WHERE id = ?",
            (utxo_id,),
            fetchone=True,
        )
        if not row:
            messagebox.showerror("Ошибка", "UTXO не найдено")
            return
        u = dict(row)
        try:
            owner = self.platform.get_user(u["owner_id"])
            owner_name = owner["name"]
        except Exception:
            owner_name = f"ID {u['owner_id']}"
        lines: list[str] = []
        lines.append(f"UTXO {u['id']}")
        lines.append("")
        lines.append("1. Формирование UTXO")
        lines.append(f"  Владелец: {owner_name} (ID {u['owner_id']})")
        lines.append(f"  Сумма: {u['amount']:.2f} ЦР")
        lines.append(f"  Статус: {self._translate_status(u['status'])}")
        created_tx = None
        if u.get("created_tx_id"):
            created_tx = self.platform.db.execute(
                "SELECT id, tx_type, channel, timestamp FROM transactions WHERE id = ?",
                (u["created_tx_id"],),
                fetchone=True,
            )
        if created_tx:
            created_tx = dict(created_tx)
            lines.append(
                f"  Создано транзакцией: {created_tx['id']} "
                f"(тип={created_tx['tx_type']}, канал={created_tx['channel']}, время={created_tx['timestamp']})"
            )
        else:
            lines.append(f"  Создано транзакцией: {u.get('created_tx_id') or '-'}")
        lines.append("")
        lines.append("2. Дальнейшее использование")
        if u.get("spent_tx_id"):
            lines.append(f"  Потрачено транзакцией: {u['spent_tx_id']}")
        else:
            lines.append("  UTXO ещё не было потрачено (UNSPENT).")
        lines.append("")
        lines.append("3. Техническая роль UTXO в модели")
        lines.append("  Запись UTXO хранит (id, owner_id, amount, status, created_tx_id, spent_tx_id, created_at, spent_at).")
        lines.append("  При формировании транзакции набор UTXO выбирается как вход (input set), помечается статусом SPENT,")
        lines.append("  а при необходимости создаётся одно новое UTXO на сдачу. При оффлайн‑сценариях именно по полям")
        lines.append("  owner_id, status и spent_tx_id моделируется ошибка двойной траты и проверяется достаточность средств.")
        export_payload = {
            "type": "utxo",
            "id": u["id"],
            "owner_id": u["owner_id"],
            "amount": u["amount"],
            "status": u["status"],
            "created_tx_id": u["created_tx_id"],
            "spent_tx_id": u.get("spent_tx_id"),
        }
        bank_id = owner["bank_id"] if "bank_id" in owner else None
        self._show_steps_window(
            "Этапы формирования и использования UTXO",
            lines,
            export_handler=None,
            export_plain_handler=None,
        )


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
            "LEADER": "Лидер (ЦБ РФ)",
            "FOLLOWER": "Последователь",
            "SIGN_REQUEST": "Запрос подписи",
            "VOTE_GRANTED": "Голос получен",
            "VOTE_DENIED": "Голос отклонен",
            "APPEND_ENTRIES": "Добавление записей",
            "ENTRY_APPLIED": "Запись применена",
            "REPLICATION": "Репликация",
            "COMMITTED": "Зафиксировано",
            "LEADER_APPEND": "Лидер добавил запись",
            "TX": "Транзакция",
            "LAG": "Задержка",
            "FAULT": "Ошибка",
        }
        # Игнорируем состояния выборов - ЦБ всегда лидер
        if state in {"CANDIDATE", "ELECTION_START", "LEADER_ELECTED", "ELECTION_FAILED"}:
            return "Лидер (ЦБ РФ)"  # Всегда показываем как лидер
        return mapping.get(state, state)

    def _add_copy_menu(self, widget) -> None:
        """Добавляет контекстное меню с копированием для Text виджетов и таблиц"""
        def copy_text():
            try:
                if isinstance(widget, tk.Text):
                    # Для Text виджетов
                    if widget.tag_ranges(tk.SEL):
                        text = widget.get(tk.SEL_FIRST, tk.SEL_LAST)
                    else:
                        text = widget.get("1.0", tk.END)
                elif isinstance(widget, ttk.Treeview):
                    # Для таблиц (Treeview)
                    selection = widget.selection()
                    lines = []
                    
                    # Добавляем заголовки колонок
                    columns = widget["columns"]
                    if columns:
                        headers = [widget.heading(col, "text") or col for col in columns]
                        lines.append("\t".join(headers))
                    
                    if selection:
                        # Копируем выбранные строки
                        for item_id in selection:
                            values = widget.item(item_id, "values")
                            if values:
                                lines.append("\t".join(str(v) for v in values))
                    else:
                        # Копируем все данные таблицы
                        for item_id in widget.get_children():
                            values = widget.item(item_id, "values")
                            if values:
                                lines.append("\t".join(str(v) for v in values))
                    text = "\n".join(lines)
                else:
                    return
                
                if text:
                    self.clipboard_clear()
                    self.clipboard_append(text)
            except Exception as e:
                print(f"Ошибка при копировании: {e}")
        
        def show_menu(event):
            menu = tk.Menu(self, tearoff=0)
            menu.add_command(label="Копировать", command=copy_text)
            try:
                menu.tk_popup(event.x_root, event.y_root)
            finally:
                menu.grab_release()
        
        widget.bind("<Button-3>", show_menu)  # Правая кнопка мыши
        widget.bind("<Control-c>", lambda e: copy_text())  # Ctrl+C

    def _add_entry_menu(self, widget) -> None:
        """Добавляет контекстное меню и поддержку вставки для Entry виджетов"""
        def copy_text(event=None):
            try:
                if widget.selection_present():
                    text = widget.selection_get()
                else:
                    text = widget.get()
                if text:
                    self.clipboard_clear()
                    self.clipboard_append(text)
                return "break"  # Предотвращаем стандартную обработку
            except Exception as e:
                print(f"Ошибка при копировании: {e}")
                return "break"
        
        def cut_text(event=None):
            try:
                if widget.selection_present():
                    text = widget.selection_get()
                    self.clipboard_clear()
                    self.clipboard_append(text)
                    widget.delete(tk.SEL_FIRST, tk.SEL_LAST)
                return "break"  # Предотвращаем стандартную обработку
            except Exception as e:
                print(f"Ошибка при вырезании: {e}")
                return "break"
        
        def paste_text(event=None):
            try:
                text = self.clipboard_get()
                if text:
                    # Удаляем выделенный текст, если есть
                    if widget.selection_present():
                        widget.delete(tk.SEL_FIRST, tk.SEL_LAST)
                    # Вставляем в позицию курсора
                    widget.insert(tk.INSERT, text)
                return "break"  # Предотвращаем стандартную обработку
            except Exception as e:
                print(f"Ошибка при вставке: {e}")
                return "break"
        
        def select_all(event=None):
            widget.select_range(0, tk.END)
            widget.icursor(tk.END)
            return "break"  # Предотвращаем стандартную обработку
        
        def show_menu(event):
            menu = tk.Menu(self, tearoff=0)
            menu.add_command(label="Вырезать", command=cut_text)
            menu.add_command(label="Копировать", command=copy_text)
            menu.add_command(label="Вставить", command=paste_text)
            menu.add_separator()
            menu.add_command(label="Выделить всё", command=select_all)
            try:
                menu.tk_popup(event.x_root, event.y_root)
            finally:
                menu.grab_release()
        
        widget.bind("<Button-3>", show_menu)  # Правая кнопка мыши
        widget.bind("<Control-c>", copy_text)  # Ctrl+C
        widget.bind("<Control-x>", cut_text)  # Ctrl+X
        widget.bind("<Control-v>", paste_text)  # Ctrl+V
        widget.bind("<Control-a>", select_all)  # Ctrl+A

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
        self._add_copy_menu(tree)
        return tree

    def refresh_all(self) -> None:
        try:
            self._refresh_user_lists()
            self._refresh_tables()
            self._refresh_consensus_canvas()
            self._refresh_errors_table()  # Обновляем вкладку ЦБ и ошибки
            # Запускаем анимацию консенсуса, если она еще не запущена
            if self.consensus_canvas and self._consensus_anim_job is None:
                self._start_consensus_animation()
        except Exception as e:
            # Логируем ошибку, но не прерываем работу приложения
            import traceback
            print(f"Ошибка при обновлении данных: {e}")
            traceback.print_exc()

    def _refresh_user_lists(self) -> None:
        users = self.platform.list_users()
        formatted = [
            f"{u['id']} | {u['name']} ({self._user_type_label(u['user_type'])})" for u in users
        ]

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
        for combo in [self.bank_combo, self.contract_bank_combo, self.bank_filter_combo, self.online_bank_combo, self.offline_bank_combo]:
            if combo:
                old = combo.get()
                combo["values"] = bank_values
                if old and old in bank_values:
                    combo.set(old)
                elif not combo.get() and bank_values:
                    combo.current(0)

        self._refresh_online_combos()

    def _clear_tree(self, tree) -> None:
        """Очищает Treeview от всех элементов"""
        if tree:
            for item in tree.get_children():
                tree.delete(item)

    def _refresh_tables(self) -> None:
        if self.user_table:
            self._clear_tree(self.user_table)
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
            self._clear_tree(self.tx_table)
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
            self._clear_tree(self.offline_table)
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
            self._clear_tree(self.contract_table)
            for sc in self.platform.get_smart_contracts():
                creator = self.platform.get_user(sc["creator_id"])
                beneficiary = self.platform.get_user(sc["beneficiary_id"])
                bank = self.platform._get_bank(sc["bank_id"])
                status_map = {
                    "EXECUTED": "Исполнен",
                    "SCHEDULED": "Запланирован",
                    "FAILED": "Ошибка",
                }
                status = status_map.get(sc["status"], sc["status"])
                if sc["status"] == "EXECUTED" and sc.get("last_execution"):
                    status = f"Исполнен ({sc['last_execution']})"
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
                        status,
                    ),
                )

        if self.block_table:
            self._clear_tree(self.block_table)
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
                        row["tx_count"],
                        row["timestamp"],
                    ),
                )

        if self.utxo_table:
            self._clear_tree(self.utxo_table)
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
            self._refresh_bank_transactions()

        if self.issuance_table:
            self._clear_tree(self.issuance_table)
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
            self._clear_tree(self.consensus_table)
            events = self.platform.consensus.get_recent_events(limit=100)
            # Группируем события по блокам для лучшего отображения
            for event in events:
                # Фильтруем события выборов - ЦБ всегда лидер, выборов не происходит
                if event.state in {"CANDIDATE", "ELECTION_START", "LEADER_ELECTED", "ELECTION_FAILED"}:
                    continue  # Пропускаем события выборов
                
                # Форматируем время для отображения
                try:
                    from datetime import datetime
                    dt = datetime.fromisoformat(event.created_at.replace('Z', '+00:00'))
                    time_str = dt.strftime("%H:%M:%S")
                except:
                    time_str = event.created_at[-8:] if len(event.created_at) >= 8 else event.created_at
                
                self.consensus_table.insert(
                    "",
                    tk.END,
                    values=(
                        event.block_hash[:16] + "...",
                        event.event,
                        event.actor,
                        self._translate_consensus_state(event.state),
                        time_str,
                    ),
                )
        
        # Запускаем анимацию консенсуса, если она еще не запущена
        if self.consensus_canvas and self._consensus_anim_job is None:
            self._start_consensus_animation()

        if self.activity_text:
            self.activity_text.delete("1.0", tk.END)
            
            # Настройка тегов для форматирования с учетом масштаба
            widget_id = id(self.activity_text)
            zoom_factor = self._text_zoom_factors.get(widget_id, {}).get('zoom_factor', 1.0)
            base_size = self._text_zoom_factors.get(widget_id, {}).get('base_font_size', 8)
            font_size = max(6, int(base_size * zoom_factor))
            
            # Создаем шрифты с учетом масштаба
            header_font = tkfont.Font(size=int(font_size * 1.25), weight="bold")
            subheader_font = tkfont.Font(size=int(font_size * 1.1), weight="bold")
            normal_font = tkfont.Font(size=font_size)
            normal_bold_font = tkfont.Font(size=font_size, weight="bold")
            small_font = tkfont.Font(size=int(font_size * 0.9))
            
            self.activity_text.tag_configure("header", foreground="#1e40af", font=header_font)
            self.activity_text.tag_configure("subheader", foreground="#059669", font=subheader_font)
            self.activity_text.tag_configure("conflict", foreground="red", font=subheader_font)
            self.activity_text.tag_configure("stage", foreground="#4b5563", font=normal_font)
            self.activity_text.tag_configure("details", foreground="#6b7280", font=small_font)
            self.activity_text.tag_configure("separator", foreground="#9ca3af", font=small_font)
            self.activity_text.tag_configure("time", foreground="#9ca3af", font=small_font)
            self.activity_text.tag_configure("context", foreground="#7c3aed", font=normal_bold_font)
            self.activity_text.tag_configure("actor", foreground="#059669", font=normal_font)
            
            # Получаем все логи в хронологическом порядке
            all_entries = self.platform.get_activity_log(limit=1000)
            
            if not all_entries:
                self.activity_text.insert(tk.END, "Журнал активности пуст.\n", "details")
                return
            
            # Применяем фильтры и поиск
            filter_value = self.activity_filter_combo.get() if hasattr(self, 'activity_filter_combo') and self.activity_filter_combo else "Все"
            search_text = self.activity_search_entry.get().lower() if hasattr(self, 'activity_search_entry') and self.activity_search_entry else ""
            
            entries = []
            for entry in all_entries:
                context = entry.get("context", "Общее")
                stage = entry.get("stage", "")
                details = entry.get("details", "")
                actor = entry.get("actor", "")
                
                # Фильтр по контексту
                if filter_value != "Все":
                    context_map = {
                        "Транзакции": "Транзакция",
                        "Смарт-контракты": "Смарт-контракт",
                        "Эмиссия": "Эмиссия",
                        "Блоки": "Блок",
                        "Консенсус": "Консенсус",
                    }
                    if context_map.get(filter_value) != context:
                        continue
                
                # Поиск по ключевым словам
                if search_text:
                    searchable = f"{context} {stage} {details} {actor}".lower()
                    if search_text not in searchable:
                        continue
                
                entries.append(entry)
            
            if not entries:
                self.activity_text.insert(tk.END, f"Нет записей, соответствующих фильтру '{filter_value}' и поиску '{search_text}'.\n", "details")
                return
            
            # Отображаем все логи последовательно как события системы
            for entry in entries:
                stage = entry.get("stage", "")
                details = entry.get("details", "")
                actor = entry.get("actor", "")
                context = entry.get("context", "Общее")
                created_at = entry.get("created_at", "")
                
                # Форматируем время
                try:
                    from datetime import datetime
                    dt = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
                    time_str = dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]  # Дата и время с миллисекундами
                except:
                    time_str = created_at if created_at else ""
                
                # Проверяем на конфликты и ошибки
                lower = (stage + details).lower()
                is_conflict = "конфликт" in lower or "двойной трат" in lower or "ошибка" in lower or "error" in lower
                
                # Формируем строку события в формате лога
                log_parts = []
                if time_str:
                    log_parts.append(f"[{time_str}]")
                if context and context != "Общее":
                    log_parts.append(f"[{context}]")
                if stage:
                    log_parts.append(f"[{stage}]")
                if actor and actor != "Система":
                    log_parts.append(f"[{actor}]")
                
                # Основная строка события
                event_line = " ".join(log_parts)
                if details:
                    event_line += f" {details}"
                
                # Вставляем строку события
                self.activity_text.insert(tk.END, event_line + "\n", "conflict" if is_conflict else "details")
            
            # Прокручиваем в начало для хронологического просмотра
            self.activity_text.see("1.0")
    
    def _format_context_name(self, context: str) -> str:
        """Форматирует название контекста для отображения"""
        context_map = {
            "Транзакция": "📝 ТРАНЗАКЦИЯ",
            "Оффлайн-транзакция": "📱 ОФФЛАЙН-ТРАНЗАКЦИЯ",
            "Смарт-контракт": "📄 СМАРТ-КОНТРАКТ",
            "Эмиссия": "💰 ЭМИССИЯ",
            "Блок": "🔗 БЛОК",
            "Консенсус": "🤝 КОНСЕНСУС",
            "Репликация": "🔄 РЕПЛИКАЦИЯ",
            "Распределенный реестр": "📚 РАСПРЕДЕЛЕННЫЙ РЕЕСТР",
            "Общее": "⚙️ ОБЩЕЕ",
        }
        return context_map.get(context, f"📌 {context.upper()}")

    def _refresh_errors_table(self) -> None:
        if self.errors_table:
            self._clear_tree(self.errors_table)
            try:
                failed_txs = self.platform.get_failed_transactions()
                system_errors = self.platform.get_system_errors()
            except Exception as e:
                failed_txs = []
                system_errors = []
            for ftx in failed_txs:
                error_type = ftx['error_type']
                tx_id = ftx.get("tx_id") or "-"
                contract_id = ftx.get("contract_id") or None
                if contract_id:
                    type_label = f"Смарт-контракт {contract_id}"
                    context_str = f"Контракт: {contract_id}" + (f", TX: {tx_id}" if tx_id != "-" else "")
                else:
                    type_label = f"Транзакция {tx_id}"
                    context_str = f"TX: {tx_id}" if tx_id != "-" else "-"
                self.errors_table.insert(
                    "",
                    tk.END,
                    values=(
                        f"{type_label} ({error_type})",
                        ftx["error_message"],
                        context_str,
                        ftx["created_at"],
                    ),
                )
            for err in system_errors:
                self.errors_table.insert(
                    "",
                    tk.END,
                    values=(
                        f"Система: {err['error_type']}",
                        err["error_message"],
                        err.get("context", "-") or "-",
                        err["created_at"],
                    ),
                )

        if self.cbr_log:
            self.cbr_log.delete("1.0", tk.END)
            
            # Настройка тегов для форматирования с учетом масштаба
            widget_id = id(self.cbr_log)
            zoom_factor = self._text_zoom_factors.get(widget_id, {}).get('zoom_factor', 1.0)
            base_size = self._text_zoom_factors.get(widget_id, {}).get('base_font_size', 8)
            font_size = max(6, int(base_size * zoom_factor))
            
            # Создаем шрифты с учетом масштаба
            header_font = tkfont.Font(size=int(font_size * 1.1), weight="bold")
            normal_font = tkfont.Font(size=font_size)
            normal_bold_font = tkfont.Font(size=font_size, weight="bold")
            small_font = tkfont.Font(size=int(font_size * 0.9))
            
            self.cbr_log.tag_configure("header", foreground="#1e40af", font=header_font)
            self.cbr_log.tag_configure("stage", foreground="#059669", font=header_font)
            self.cbr_log.tag_configure("details", foreground="#4b5563", font=small_font)
            self.cbr_log.tag_configure("time", foreground="#9ca3af", font=small_font)
            self.cbr_log.tag_configure("actor", foreground="#7c3aed", font=small_font)
            self.cbr_log.tag_configure("context", foreground="#dc2626", font=normal_bold_font)
            self.cbr_log.tag_configure("separator", foreground="#9ca3af", font=small_font)
            
            # Получаем ВСЕ логи системы - ЦБ видит все действия
            all_entries = self.platform.get_activity_log(limit=2000)
            
            if not all_entries:
                self.cbr_log.insert(tk.END, "Журнал событий ЦБ пуст. Выполните действия в системе для генерации логов.\n", "details")
                return
            
            # Применяем фильтры
            filter_value = self.cbr_filter_combo.get() if hasattr(self, 'cbr_filter_combo') and self.cbr_filter_combo else "Все"
            search_text = self.cbr_search_entry.get().lower() if hasattr(self, 'cbr_search_entry') and self.cbr_search_entry else ""
            
            entries = []
            for entry in all_entries:
                context = entry.get("context", "Общее")
                stage = entry.get("stage", "")
                details = entry.get("details", "")
                actor = entry.get("actor", "")
                
                # Фильтр по контексту
                if filter_value != "Все":
                    context_map = {
                        "Транзакции": "Транзакция",
                        "Смарт-контракты": "Смарт-контракт",
                        "Эмиссия": "Эмиссия",
                        "Блоки": "Блок",
                        "Консенсус": "Консенсус",
                    }
                    if context_map.get(filter_value) != context:
                        continue
                
                # Поиск по ключевым словам
                if search_text:
                    searchable = f"{context} {stage} {details} {actor}".lower()
                    if search_text not in searchable:
                        continue
                
                entries.append(entry)
            
            if not entries:
                self.cbr_log.insert(tk.END, f"Нет записей, соответствующих фильтру '{filter_value}' и поиску '{search_text}'.\n", "details")
                return
            
            # Отображаем все логи последовательно с детальной структурой
            prev_context = None
            
            for entry in entries:
                stage = entry.get("stage", "")
                details = entry.get("details", "")
                actor = entry.get("actor", "Система")
                context = entry.get("context", "Общее")
                created_at = entry.get("created_at", "")
                
                # Форматируем время
                try:
                    from datetime import datetime
                    dt = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
                    time_str = dt.strftime("%H:%M:%S.%f")[:-3]  # Миллисекунды
                except:
                    time_str = created_at[-12:] if len(created_at) >= 12 else created_at
                
                # Разделитель при смене контекста
                if prev_context and prev_context != context:
                    separator = f"\n{'='*100}\n"
                    self.cbr_log.insert(tk.END, separator, "separator")
                
                # Заголовок контекста (если изменился)
                if prev_context != context:
                    context_display = self._format_context_name(context)
                    header_text = f"{context_display}\n"
                    self.cbr_log.insert(tk.END, header_text, "header")
                
                # Этап с временем
                stage_text = f"  [{time_str}] {stage}\n"
                self.cbr_log.insert(tk.END, stage_text, "stage")
                
                # Актор
                if actor and actor != "Система":
                    actor_text = f"    Актор: {actor}\n"
                    self.cbr_log.insert(tk.END, actor_text, "actor")
                
                # Детали (полностью, без обрезания)
                if details:
                    # Разбиваем детали на строки для лучшей читаемости
                    detail_lines = details.split(", ")
                    for detail_line in detail_lines:
                        if detail_line.strip():
                            detail_text = f"    {detail_line.strip()}\n"
                            self.cbr_log.insert(tk.END, detail_text, "details")
                
                # Пустая строка между записями
                self.cbr_log.insert(tk.END, "\n", "separator")
                
                prev_context = context
            
            # Прокручиваем в начало для хронологического просмотра
            self.cbr_log.see("1.0")
    
    def _export_cbr_log_csv(self) -> None:
        """Экспорт журнала ЦБ в CSV (использует те же отфильтрованные данные, что и журнал)"""
        try:
            # Получаем те же данные, что и в журнале (с учетом фильтров)
            all_entries = self.platform.get_activity_log(limit=2000)
            if not all_entries:
                messagebox.showinfo("Экспорт", "Нет данных для экспорта")
                return
            
            # Применяем те же фильтры, что и в журнале
            filter_value = self.cbr_filter_combo.get() if hasattr(self, 'cbr_filter_combo') and self.cbr_filter_combo else "Все"
            search_text = self.cbr_search_entry.get().lower() if hasattr(self, 'cbr_search_entry') and self.cbr_search_entry else ""
            
            entries = []
            for entry in all_entries:
                context = entry.get("context", "Общее")
                stage = entry.get("stage", "")
                details = entry.get("details", "")
                actor = entry.get("actor", "")
                
                # Фильтр по контексту
                if filter_value != "Все":
                    context_map = {
                        "Транзакции": "Транзакция",
                        "Смарт-контракты": "Смарт-контракт",
                        "Эмиссия": "Эмиссия",
                        "Блоки": "Блок",
                        "Консенсус": "Консенсус",
                    }
                    if context_map.get(filter_value) != context:
                        continue
                
                # Поиск по ключевым словам
                if search_text:
                    searchable = f"{context} {stage} {details} {actor}".lower()
                    if search_text not in searchable:
                        continue
                
                entries.append(entry)
            
            if not entries:
                messagebox.showinfo("Экспорт", "Нет данных для экспорта с примененными фильтрами")
                return
            
            filename = filedialog.asksaveasfilename(
                defaultextension=".csv",
                filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
                title="Экспорт журнала ЦБ в CSV"
            )
            if not filename:
                return
            
            import csv
            with open(filename, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(["Время", "Контекст", "Этап", "Актор", "Детали"])
                for entry in entries:
                    writer.writerow([
                        entry.get("created_at", ""),
                        entry.get("context", ""),
                        entry.get("stage", ""),
                        entry.get("actor", ""),
                        entry.get("details", "")
                    ])
            messagebox.showinfo("Экспорт", f"Журнал ЦБ экспортирован в {filename} ({len(entries)} записей)")
        except Exception as e:
            messagebox.showerror("Ошибка экспорта", f"Ошибка при экспорте: {e}")
    
    def _export_cbr_log_json(self) -> None:
        """Экспорт журнала ЦБ в JSON (использует те же отфильтрованные данные, что и журнал)"""
        try:
            # Получаем те же данные, что и в журнале (с учетом фильтров)
            all_entries = self.platform.get_activity_log(limit=2000)
            if not all_entries:
                messagebox.showinfo("Экспорт", "Нет данных для экспорта")
                return
            
            # Применяем те же фильтры, что и в журнале
            filter_value = self.cbr_filter_combo.get() if hasattr(self, 'cbr_filter_combo') and self.cbr_filter_combo else "Все"
            search_text = self.cbr_search_entry.get().lower() if hasattr(self, 'cbr_search_entry') and self.cbr_search_entry else ""
            
            entries = []
            for entry in all_entries:
                context = entry.get("context", "Общее")
                stage = entry.get("stage", "")
                details = entry.get("details", "")
                actor = entry.get("actor", "")
                
                # Фильтр по контексту
                if filter_value != "Все":
                    context_map = {
                        "Транзакции": "Транзакция",
                        "Смарт-контракты": "Смарт-контракт",
                        "Эмиссия": "Эмиссия",
                        "Блоки": "Блок",
                        "Консенсус": "Консенсус",
                    }
                    if context_map.get(filter_value) != context:
                        continue
                
                # Поиск по ключевым словам
                if search_text:
                    searchable = f"{context} {stage} {details} {actor}".lower()
                    if search_text not in searchable:
                        continue
                
                entries.append(entry)
            
            if not entries:
                messagebox.showinfo("Экспорт", "Нет данных для экспорта с примененными фильтрами")
                return
            
            filename = filedialog.asksaveasfilename(
                defaultextension=".json",
                filetypes=[("JSON files", "*.json"), ("All files", "*.*")],
                title="Экспорт журнала ЦБ в JSON"
            )
            if not filename:
                return
            
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(entries, f, ensure_ascii=False, indent=2)
            messagebox.showinfo("Экспорт", f"Журнал ЦБ экспортирован в {filename} ({len(entries)} записей)")
        except Exception as e:
            messagebox.showerror("Ошибка экспорта", f"Ошибка при экспорте: {e}")
    
    def _export_activity_log_csv(self) -> None:
        """Экспорт журнала этапов в CSV"""
        try:
            entries = self.platform.get_activity_log(limit=1000)
            if not entries:
                messagebox.showinfo("Экспорт", "Нет данных для экспорта")
                return
            
            filename = filedialog.asksaveasfilename(
                defaultextension=".csv",
                filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
                title="Экспорт журнала этапов в CSV"
            )
            if not filename:
                return
            
            import csv
            with open(filename, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(["Время", "Контекст", "Этап", "Актор", "Детали"])
                for entry in entries:
                    writer.writerow([
                        entry.get("created_at", ""),
                        entry.get("context", ""),
                        entry.get("stage", ""),
                        entry.get("actor", ""),
                        entry.get("details", "")
                    ])
            messagebox.showinfo("Экспорт", f"Журнал этапов экспортирован в {filename}")
        except Exception as e:
            messagebox.showerror("Ошибка экспорта", f"Ошибка при экспорте: {e}")
    
    def _export_activity_log_json(self) -> None:
        """Экспорт журнала этапов в JSON"""
        try:
            entries = self.platform.get_activity_log(limit=1000)
            if not entries:
                messagebox.showinfo("Экспорт", "Нет данных для экспорта")
                return
            
            filename = filedialog.asksaveasfilename(
                defaultextension=".json",
                filetypes=[("JSON files", "*.json"), ("All files", "*.*")],
                title="Экспорт журнала этапов в JSON"
            )
            if not filename:
                return
            
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(entries, f, ensure_ascii=False, indent=2)
            messagebox.showinfo("Экспорт", f"Журнал этапов экспортирован в {filename}")
        except Exception as e:
            messagebox.showerror("Ошибка экспорта", f"Ошибка при экспорте: {e}")

    def _refresh_consensus_canvas(self) -> None:
        try:
            canvas = self.consensus_canvas
            if not canvas:
                return
            canvas.delete("all")
            nodes = self.platform.consensus.get_nodes()
            if not nodes or len(nodes) == 0:
                canvas.create_text(
                    200,
                    80,
                    text="Нет узлов. Добавьте банки, чтобы увидеть визуализацию консенсуса.",
                    fill="gray",
                )
                return
            width = int(canvas.winfo_width() or 1200)
            leader = nodes[0]
            bank_nodes = nodes[1:]

            active_actor = self._consensus_active_actor
            if active_actor is None:
                recent_events = self.platform.consensus.get_recent_events(limit=1)
                active_actor = recent_events[0].actor if recent_events and len(recent_events) > 0 else None

            leader_x = width // 2
            leader_y = 120

            # ЦБ всегда лидер, не выходит из строя
            if self._consensus_active_state in {"LEADER", "LEADER_APPEND"}:
                leader_fill = "#10b981"
            else:
                leader_fill = "#10b981"  # ЦБ всегда лидер (зеленый)
            canvas.create_oval(
                leader_x - 45, leader_y - 45, leader_x + 45, leader_y + 45, fill=leader_fill, outline="#0f172a", width=2
            )
            canvas.create_text(leader_x, leader_y, text=leader, fill="black", width=140)

            if bank_nodes:
                min_spacing = 120
                calculated_spacing = width // (len(bank_nodes) + 1)
                spacing = max(calculated_spacing, min_spacing)
                if spacing < min_spacing:
                    spacing = min_spacing
                y_banks = 220
            
            # Определяем активные узлы на текущем этапе
            active_nodes = self._consensus_active_nodes if hasattr(self, '_consensus_active_nodes') else set()
            if active_actor and active_actor != leader and active_actor in bank_nodes:
                active_nodes.add(active_actor)
            
            # Сохраняем координаты узлов для анимации стрелок
            node_positions = {}
            
            for idx, node in enumerate(bank_nodes, start=1):
                x = spacing * idx
                if x + 35 > width - 10:
                    total_width = spacing * len(bank_nodes)
                    start_x = (width - total_width) // 2
                    x = start_x + spacing * (idx - 1) + spacing // 2
                
                node_positions[node] = (x, y_banks)
                
                # Определяем цвет узла в зависимости от активности
                if node in active_nodes or node == active_actor:
                    fill_color = "#10b981"  # Зеленый для активных
                    outline_width = 3
                else:
                    fill_color = "#2563eb"  # Синий для неактивных
                    outline_width = 2
                
                canvas.create_oval(
                    x - 35, y_banks - 35, x + 35, y_banks + 35, 
                    fill=fill_color, outline="#0f172a", width=outline_width
                )
                canvas.create_text(x, y_banks, text=node, fill="white", font=("TkDefaultFont", 9, "bold"), width=120)
                
                # Рисуем стрелку только для активных узлов на текущем этапе
                if node in active_nodes or node == active_actor:
                    # Анимированная стрелка от ЦБ к активному узлу
                    line_color = "#10b981"
                    line_width = 3
                    # Рисуем стрелку с анимацией (более яркая для активных)
                    canvas.create_line(
                        leader_x,
                        leader_y + 45,
                        x,
                        y_banks - 35,
                        arrow=tk.LAST,
                        fill=line_color,
                        width=line_width,
                        arrowshape=(10, 12, 3)
                    )
                elif self._consensus_active_state in {"SIGN_REQUEST", "VOTE_GRANTED", "REPLICATION", "APPEND_ENTRIES"}:
                    # Показываем серую пунктирную стрелку для неактивных узлов во время запросов
                    canvas.create_line(
                        leader_x,
                        leader_y + 45,
                        x,
                        y_banks - 35,
                        arrow=tk.LAST,
                        fill="#9ca3af",
                        width=1,
                        dash=(5, 5)
                    )
            subtitle = self._consensus_active_event or ""
            if self._consensus_votes is not None and self._consensus_total_banks is not None:
                votes = self._consensus_votes
                replications = self._consensus_replications or 0
                total_banks = self._consensus_total_banks
            else:
                votes = 0
                replications = 0
                total_banks = max(len(bank_nodes), 1)
            canvas.create_text(
                width // 2,
                50,
                text=f"Голосов: {votes}/{total_banks} | Репликаций: {replications}/{total_banks}",
                fill="#4b5563",
            )
            if subtitle:
                canvas.create_text(
                    width // 2,
                    30,
                    text=subtitle,
                    fill="#4b5563",
                )

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
                        # Показываем хеш и количество транзакций
                        hash_text = row["hash"][:12] + "..."
                        ledger_canvas.create_text(
                            x,
                            y_l + 5,
                            text=hash_text,
                            fill="#4b5563",
                            font=("TkDefaultFont", 7),
                        )
                        
                        # Получаем количество транзакций в блоке
                        tx_count_row = self.platform.db.execute(
                            "SELECT COUNT(*) as count FROM block_transactions bt JOIN blocks b ON b.id = bt.block_id WHERE b.height = ?",
                            (row["height"],),
                            fetchone=True,
                        )
                        tx_count = tx_count_row["count"] if tx_count_row else 0
                        ledger_canvas.create_text(
                            x,
                            y_l + 18,
                            text=f"{tx_count} TX",
                            fill="#059669",
                            font=("TkDefaultFont", 7, "bold"),
                        )
                        
                        # Интерактивная навигация - делаем блок кликабельным
                        def on_block_click(event, block_height=row["height"]):
                            # Находим блок в таблице и выделяем его
                            if self.block_table:
                                for item in self.block_table.get_children():
                                    values = self.block_table.item(item, "values")
                                    if values and str(block_height) in str(values[0]):
                                        self.block_table.selection_set(item)
                                        self.block_table.see(item)
                                        self._on_block_row_double_click(None)
                                        break
                        
                        # Создаем невидимую область для клика
                        click_area = ledger_canvas.create_rectangle(
                            x0, y0, x1, y1, fill="", outline="", width=0
                        )
                        ledger_canvas.tag_bind(click_area, "<Button-1>", on_block_click)
                        ledger_canvas.tag_bind(click_area, "<Enter>", lambda e, x=x, y=y_l: ledger_canvas.create_text(x, y-40, text=f"Кликните для деталей", fill="#059669", font=("TkDefaultFont", 8), tags="tooltip"))
                        ledger_canvas.tag_bind(click_area, "<Leave>", lambda e: ledger_canvas.delete("tooltip"))
                        
                        if prev_x is not None:
                            # Улучшенная визуализация связи блоков
                            ledger_canvas.create_line(
                                prev_x + 60, prev_y, x - 60, y_l, 
                                arrow=tk.LAST, 
                                fill="#6b7280",
                                width=2,
                                arrowshape=(8, 10, 3)
                            )
                            # Показываем хеш связи
                            mid_x = (prev_x + 60 + x - 60) // 2
                            mid_y = (prev_y + y_l) // 2
                            prev_hash = row.get("previous_hash") if isinstance(row, dict) else (row["previous_hash"] if "previous_hash" in row.keys() else None)
                            prev_hash_short = prev_hash[:8] + "..." if prev_hash else "Genesis"
                            ledger_canvas.create_text(
                                mid_x, mid_y - 8,
                                text=prev_hash_short,
                                fill="#9ca3af",
                                font=("TkDefaultFont", 6),
                            )
                        prev_x, prev_y = x, y_l
        except Exception as e:
            # Обрабатываем ошибки при обновлении canvas
            import traceback
            print(f"Ошибка при обновлении canvas консенсуса: {e}")
            traceback.print_exc()

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
            self._consensus_anim_events = []
            self._consensus_anim_index = 0
            self._consensus_active_actor = None
            self._consensus_active_state = None
            self._consensus_active_event = None
            self._consensus_active_nodes = set()
            self._ledger_active_height = None
            self._refresh_consensus_canvas()
            return
        rows = self.platform.db.execute(
            """
            SELECT block_hash, event, actor, state, created_at
            FROM consensus_events
            WHERE block_hash = ?
            AND state NOT IN ('CANDIDATE', 'ELECTION_START', 'LEADER_ELECTED', 'ELECTION_FAILED')
            ORDER BY id ASC
            """,
            (last_block,),
            fetchall=True,
        )
        # Фильтруем события выборов - ЦБ всегда лидер
        filtered_events = []
        for r in rows if rows else []:
            event_dict = dict(r)
            if event_dict.get("state") not in {"CANDIDATE", "ELECTION_START", "LEADER_ELECTED", "ELECTION_FAILED"}:
                filtered_events.append(event_dict)
        self._consensus_anim_events = filtered_events
        self._consensus_anim_index = 0
        self._consensus_active_nodes = set()
        if self._consensus_anim_events:
            self._run_consensus_animation_step()
        else:
            self._refresh_consensus_canvas()

    def _run_consensus_animation_step(self) -> None:
        if not self.consensus_canvas:
            self._consensus_anim_job = None
            return
        
        # Если нет событий, просто обновляем canvas без анимации
        if not self._consensus_anim_events:
            self._consensus_active_actor = None
            self._consensus_active_state = None
            self._consensus_active_event = None
            self._consensus_active_nodes = set()
            self._consensus_votes = 0
            self._consensus_replications = 0
            nodes = self.platform.consensus.get_nodes()
            self._consensus_total_banks = max(len(nodes) - 1, 1)
            self._refresh_consensus_canvas()
            self._consensus_anim_job = None
            return
        
        event = self._consensus_anim_events[self._consensus_anim_index]
        self._consensus_active_actor = event["actor"]
        self._consensus_active_state = event["state"]
        self._consensus_active_event = event["event"]
        
        # Определяем активные узлы на текущем этапе
        self._consensus_active_nodes = set()
        if event["state"] in {"SIGN_REQUEST", "VOTE_GRANTED", "REPLICATION", "APPEND_ENTRIES"}:
            # На этапах запросов и репликации активен конкретный узел
            if event["actor"] != self.platform.consensus.node_id:
                self._consensus_active_nodes.add(event["actor"])
        elif event["state"] == "COMMITTED":
            # На этапе фиксации активны все узлы
            nodes = self.platform.consensus.get_nodes()
            self._consensus_active_nodes = set(nodes[1:])  # Все банки кроме лидера
        
        if self._ledger_last_rows:
            idx = self._consensus_anim_index % len(self._ledger_last_rows)
            self._ledger_active_height = self._ledger_last_rows[idx]["height"]
        else:
            self._ledger_active_height = None
        seen = self._consensus_anim_events[: self._consensus_anim_index + 1]
        self._consensus_votes = sum(1 for e in seen if e["state"] == "VOTE_GRANTED")
        self._consensus_replications = sum(1 for e in seen if e["state"] == "REPLICATION")
        nodes = self.platform.consensus.get_nodes()
        self._consensus_total_banks = max(len(nodes) - 1, 1)
        self._refresh_consensus_canvas()
        self._consensus_anim_index = (self._consensus_anim_index + 1) % len(
            self._consensus_anim_events
        )
        self._consensus_anim_job = self.after(1000, self._run_consensus_animation_step)  # Увеличил время для лучшей видимости

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
            bank_id = self._selected_id(self.offline_bank_combo.get()) if self.offline_bank_combo else None
            amount = float(self.offline_tx_amount.get())
            self.platform.create_offline_transaction(sender_id, receiver_id, amount, bank_id=bank_id)
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
            bank_id = self._selected_id(self.online_bank_combo.get()) if self.online_bank_combo else None
            self.platform.create_online_transaction(sender_id, receiver_id, amount, channel, bank_id=bank_id)
            self.refresh_all()
            messagebox.showinfo("Онлайн транзакция", "Онлайн транзакция успешно выполнена и записана в реестр")
        except Exception as exc:
            messagebox.showerror("Ошибка", str(exc))

    def _ui_create_contract(self) -> None:
        try:
            from datetime import datetime
            sender_id = self._selected_id(self.contract_sender_combo.get())
            receiver_id = self._selected_id(self.contract_receiver_combo.get())
            bank_id = self._selected_id(self.contract_bank_combo.get())
            amount = float(self.contract_amount.get())
            description = self.contract_description.get()
            date_str = self.contract_date.get()
            time_str = self.contract_time.get()
            try:
                next_execution = datetime.strptime(f"{date_str} {time_str}", "%Y-%m-%d %H:%M:%S")
            except ValueError:
                try:
                    next_execution = datetime.strptime(f"{date_str} {time_str}", "%Y-%m-%d %H:%M")
                except ValueError:
                    raise ValueError("Неверный формат даты/времени. Используйте YYYY-MM-DD HH:MM:SS")
            self.platform.create_smart_contract(
                sender_id, receiver_id, bank_id, amount, description, next_execution
            )
            self.refresh_all()
            messagebox.showinfo("Смарт-контракт", "Контракт создан")
        except Exception as exc:
            messagebox.showerror("Ошибка", str(exc))

    def _ui_run_contracts(self) -> None:
        executed = self.platform.execute_due_contracts(force=True)
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

    def _refresh_bank_transactions(self) -> None:
        if not self.bank_tx_table:
            return
        def clear(tree):
            if tree:
                for item in tree.get_children():
                    tree.delete(item)
        self._clear_tree(self.bank_tx_table)
        selected_bank = self.bank_filter_combo.get() if self.bank_filter_combo else None
        bank_id = None
        if selected_bank:
            bank_id = self._selected_id(selected_bank)
        for tx in self.platform.get_transactions(bank_id=bank_id):
            sender = self.platform.get_user(tx["sender_id"])
            receiver = self.platform.get_user(tx["receiver_id"])
            tx_type_display = "Смарт-контракт" if tx['tx_type'] == "CONTRACT" else (
                self._translate_channel(tx['channel']) if tx['tx_type'] == "EXCHANGE" else tx['channel']
            )
            self.bank_tx_table.insert(
                "",
                tk.END,
                values=(
                    tx["id"],
                    sender["name"],
                    receiver["name"],
                    tx_type_display,
                    f"{tx['amount']:.2f}",
                ),
            )

    def _clear_bank_filter(self) -> None:
        if self.bank_filter_combo:
            self.bank_filter_combo.set("")
        self._refresh_bank_transactions()

    def _ui_export_registry(self) -> None:
        folder = filedialog.askdirectory(title="Выберите папку для экспорта")
        if not folder:
            return
        paths = self.platform.export_registry(folder)
        messagebox.showinfo("Экспорт", f"Реестр экспортирован в файл:\n{paths['ledger']}")



if __name__ == "__main__":
    app = DigitalRubleApp()
    app.mainloop()