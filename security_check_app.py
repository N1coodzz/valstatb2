import json
import platform
import socket
import subprocess
from datetime import datetime
from pathlib import Path
import tkinter as tk
from tkinter import messagebox


class SecurityCheckApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Проверка состояния средств защиты ПК")
        self.root.geometry("840x560")
        self.root.resizable(False, False)
        self.results = {}

        title = tk.Label(
            root,
            text="Программа проверки состояния средств защиты персонального компьютера",
            font=("Arial", 14, "bold"),
        )
        title.pack(pady=10)

        buttons = tk.Frame(root)
        buttons.pack(pady=5)

        tk.Button(buttons, text="Проверить подключение к Интернету", width=36,
                  command=self.check_internet).grid(row=0, column=0, padx=5, pady=5)
        tk.Button(buttons, text="Проверить наличие межсетевого экрана", width=36,
                  command=self.check_firewall_exists).grid(row=1, column=0, padx=5, pady=5)
        tk.Button(buttons, text="Проверить работоспособность МЭ", width=36,
                  command=self.check_firewall_state).grid(row=2, column=0, padx=5, pady=5)
        tk.Button(buttons, text="Проверить наличие антивируса", width=36,
                  command=self.check_antivirus_exists).grid(row=0, column=1, padx=5, pady=5)
        tk.Button(buttons, text="Проверить работоспособность АВ", width=36,
                  command=self.check_antivirus_state).grid(row=1, column=1, padx=5, pady=5)
        tk.Button(buttons, text="Вывести итоговый результат", width=36,
                  command=self.show_report).grid(row=2, column=1, padx=5, pady=5)
        tk.Button(buttons, text="Сохранить результат в файл", width=36,
                  command=self.save_report).grid(row=3, column=0, padx=5, pady=5)
        tk.Button(buttons, text="Выход", width=36,
                  command=root.destroy).grid(row=3, column=1, padx=5, pady=5)

        label = tk.Label(root, text="Результаты проверки и рекомендации", font=("Arial", 11, "bold"))
        label.pack(pady=(8, 0))

        self.output = tk.Text(root, height=17, width=100, wrap="word", font=("Consolas", 10))
        self.output.pack(padx=10, pady=8)
        self.show_text("Для начала проверки выберите один из модулей.")

    def show_text(self, text):
        self.output.delete("1.0", tk.END)
        self.output.insert(tk.END, text)

    def run_powershell(self, command):
        if platform.system() != "Windows":
            raise RuntimeError("Проверка рассчитана на операционные системы Windows.")
        completed = subprocess.run(
            ["powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-Command", command],
            capture_output=True,
            text=True,
            timeout=15,
            encoding="cp866",
            errors="ignore",
        )
        if completed.returncode != 0 and not completed.stdout.strip():
            raise RuntimeError(completed.stderr.strip() or "Команда PowerShell не вернула данные.")
        return completed.stdout.strip()

    def check_internet(self):
        try:
            socket.create_connection(("8.8.8.8", 53), timeout=3)
            self.results["internet"] = "1. Подключение к Интернету установлено."
        except OSError:
            self.results["internet"] = "1. Подключение к Интернету отсутствует."
        self.show_text(self.results["internet"])

    def get_firewall_profiles(self):
        command = "Get-NetFirewallProfile | Select-Object Name,Enabled | ConvertTo-Json"
        output = self.run_powershell(command)
        if not output:
            return []
        data = json.loads(output)
        return data if isinstance(data, list) else [data]

    def check_firewall_exists(self):
        try:
            profiles = self.get_firewall_profiles()
            if profiles:
                names = [str(item.get("Name", "")) for item in profiles]
                self.results["firewall"] = "2. Межсетевой экран Windows обнаружен. Профили: " + ", ".join(names) + "."
            else:
                self.results["firewall"] = "2. Сведения о межсетевом экране не обнаружены."
        except Exception as exc:
            self.results["firewall"] = "2. Ошибка проверки наличия МЭ: " + str(exc)
        self.show_text(self.results["firewall"])

    def check_firewall_state(self):
        try:
            profiles = self.get_firewall_profiles()
            enabled = [p["Name"] for p in profiles if str(p.get("Enabled")) == "True"]
            if enabled:
                self.results["firewall_state"] = "3. Межсетевой экран включен для профилей: " + ", ".join(enabled) + "."
            else:
                self.results["firewall_state"] = "3. Профили межсетевого экрана отключены или состояние недоступно."
        except Exception as exc:
            self.results["firewall_state"] = "3. Ошибка проверки работоспособности МЭ: " + str(exc)
        self.show_text(self.results["firewall_state"])

    def get_antivirus_products(self):
        command = (
            "Get-CimInstance -Namespace root/SecurityCenter2 -ClassName AntivirusProduct "
            "| Select-Object displayName,productState | ConvertTo-Json"
        )
        output = self.run_powershell(command)
        if not output:
            return []
        data = json.loads(output)
        return data if isinstance(data, list) else [data]

    def check_antivirus_exists(self):
        try:
            products = self.get_antivirus_products()
            names = [p.get("displayName", "") for p in products if p.get("displayName")]
            if names:
                self.results["antivirus"] = "4. Обнаружено антивирусное ПО: " + ", ".join(names) + "."
            else:
                self.results["antivirus"] = "4. Антивирусное программное обеспечение не обнаружено."
        except Exception as exc:
            self.results["antivirus"] = "4. Ошибка проверки наличия антивируса: " + str(exc)
        self.show_text(self.results["antivirus"])

    def check_antivirus_state(self):
        try:
            products = self.get_antivirus_products()
            if products:
                states = []
                for item in products:
                    name = item.get("displayName", "Антивирус")
                    state = item.get("productState", "неизвестно")
                    states.append(f"{name}: код состояния {state}")
                self.results["antivirus_state"] = "5. Антивирусное ПО зарегистрировано в центре безопасности Windows. " + "; ".join(states) + "."
            else:
                self.results["antivirus_state"] = "5. Работоспособность антивируса не подтверждена."
        except Exception as exc:
            self.results["antivirus_state"] = "5. Ошибка проверки работоспособности антивируса: " + str(exc)
        self.show_text(self.results["antivirus_state"])

    def build_report(self):
        report = ["Результаты проверки состояния средств защиты персонального компьютера", ""]
        keys = ["internet", "firewall", "firewall_state", "antivirus", "antivirus_state"]
        for key in keys:
            report.append(self.results.get(key, "Проверка не выполнялась."))
        report.append("")
        report.append("Рекомендации: при наличии ошибок необходимо включить межсетевой экран, проверить состояние антивируса и обновить его базы.")
        return "\n".join(report)

    def show_report(self):
        self.show_text(self.build_report())

    def save_report(self):
        filename = "security_report_" + datetime.now().strftime("%Y%m%d_%H%M%S") + ".txt"
        Path(filename).write_text(self.build_report(), encoding="utf-8")
        messagebox.showinfo("Сохранение", "Отчет сохранен в файл: " + filename)


if __name__ == "__main__":
    root = tk.Tk()
    app = SecurityCheckApp(root)
    root.mainloop()
