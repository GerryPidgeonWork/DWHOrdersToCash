# ====================================================================================================
# P05_gui_elements.py
# ----------------------------------------------------------------------------------------------------
# DWH Orders-to-Cash Extractor GUI
# ----------------------------------------------------------------------------------------------------
# Purpose:
#   Provides a user interface for running the full DWH extraction process.
#   - Allows users to select a Snowflake login email (Gerry, Dimitrios, or custom)
#   - Automatically determines the reporting month (prior or current)
#   - Allows manual override of month (YYYY-MM)
#   - Streams live logs from M01_combine_sql.main() into a scrollable text box
#   - Keeps GUI open during processing
# ----------------------------------------------------------------------------------------------------
# Integration:
#   Calls main() from main/M01_combine_sql.py
#   Uses imports from processes/P00_set_packages.py
# ====================================================================================================

import sys
from pathlib import Path
import io

# Adjust sys.path for parent folder imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.dont_write_bytecode = True

# ----------------------------------------------------------------------------------------------------
# Import Project Libraries
# ----------------------------------------------------------------------------------------------------
from processes.P00_set_packages import *
from main.M01_combine_sql import main as run_dwh_main
import processes.P07_module_configs as cfg

# ----------------------------------------------------------------------------------------------------
# Redirect stdout/stderr to GUI Text Widget
# ----------------------------------------------------------------------------------------------------
class TextRedirector(io.TextIOBase):
    """Redirects stdout/stderr to a Tkinter Text widget."""
    def __init__(self, text_widget):
        self.text_widget = text_widget

    def write(self, message):
        self.text_widget.configure(state="normal")
        self.text_widget.insert("end", message)
        self.text_widget.see("end")  # auto-scroll
        self.text_widget.configure(state="disabled")

    def flush(self):
        pass


# ====================================================================================================
# CLASS DEFINITION
# ====================================================================================================

class DWHOrdersToCashGUI(tk.Tk):
    """Main Tkinter window for the DWH Orders-to-Cash extraction workflow."""

    def __init__(self):
        super().__init__()

        # --------------------------------------------------------------------------------------------
        # Window Configuration
        # --------------------------------------------------------------------------------------------
        self.title("📊 DWH Orders-to-Cash Extractor")
        self.geometry("900x700")
        self.minsize(900, 700)

        # Track user selections
        self.email_var = tk.StringVar(value="gerry")
        self.custom_email_var = tk.StringVar()
        self.month_override_var = tk.StringVar()
        self.notes_var = tk.StringVar()

        # Determine default reporting period
        self.default_month, self.start_date, self.end_date = self.get_default_month_period()

        # --------------------------------------------------------------------------------------------
        # Header Section
        # --------------------------------------------------------------------------------------------
        header = ttk.Label(
            self,
            text="📊 DWH Orders-to-Cash Extractor",
            font=("Segoe UI", 16, "bold"),
        )
        header.pack(pady=10)

        # --------------------------------------------------------------------------------------------
        # Email Selection Section
        # --------------------------------------------------------------------------------------------
        email_frame = ttk.LabelFrame(self, text="Select Snowflake Email", padding=10)
        email_frame.pack(fill="x", padx=20, pady=5)

        ttk.Radiobutton(
            email_frame, text="Gerry (Default)", variable=self.email_var, value="gerry"
        ).grid(row=0, column=0, sticky="w", padx=5, pady=2)

        ttk.Radiobutton(
            email_frame, text="Dimitrios", variable=self.email_var, value="dimitrios"
        ).grid(row=1, column=0, sticky="w", padx=5, pady=2)

        ttk.Radiobutton(
            email_frame, text="Custom", variable=self.email_var, value="custom"
        ).grid(row=2, column=0, sticky="w", padx=5, pady=2)

        self.custom_email_entry = ttk.Entry(email_frame, textvariable=self.custom_email_var, width=40)
        self.custom_email_entry.grid(row=2, column=1, padx=5, pady=2)

        # --------------------------------------------------------------------------------------------
        # Reporting Period Section
        # --------------------------------------------------------------------------------------------
        month_frame = ttk.LabelFrame(self, text="Reporting Period", padding=10)
        month_frame.pack(fill="x", padx=20, pady=5)

        default_label = ttk.Label(
            month_frame,
            text=f"Default: {self.default_month.strftime('%B %Y')} ({self.start_date} → {self.end_date})",
        )
        default_label.grid(row=0, column=0, columnspan=2, sticky="w", pady=2)

        ttk.Label(month_frame, text="Override (YYYY-MM):").grid(row=1, column=0, sticky="w", pady=2)
        ttk.Entry(month_frame, textvariable=self.month_override_var, width=10).grid(row=1, column=1, sticky="w", pady=2)

        # --------------------------------------------------------------------------------------------
        # Notes / Run Label Section
        # --------------------------------------------------------------------------------------------
        notes_frame = ttk.LabelFrame(self, text="Run Label / Notes (optional)", padding=10)
        notes_frame.pack(fill="x", padx=20, pady=5)
        ttk.Entry(notes_frame, textvariable=self.notes_var, width=60).pack(fill="x", padx=5, pady=2)

        # --------------------------------------------------------------------------------------------
        # Buttons
        # --------------------------------------------------------------------------------------------
        button_frame = ttk.Frame(self)
        button_frame.pack(fill="x", padx=20, pady=5)

        self.run_button = ttk.Button(button_frame, text="▶ Run Extraction", command=self.run_extraction)
        self.run_button.pack(side="left", padx=5)

        ttk.Button(button_frame, text="❌ Close", command=self.destroy).pack(side="right", padx=5)

        # --------------------------------------------------------------------------------------------
        # Status Output Box
        # --------------------------------------------------------------------------------------------
        status_frame = ttk.LabelFrame(self, text="Status", padding=10)
        status_frame.pack(fill="both", expand=True, padx=20, pady=10)

        self.status_box = tk.Text(status_frame, wrap="word", height=25, state="disabled", font=("Consolas", 10))
        self.status_box.pack(fill="both", expand=True, side="left")

        scrollbar = ttk.Scrollbar(status_frame, command=self.status_box.yview)
        scrollbar.pack(side="right", fill="y")
        self.status_box.config(yscrollcommand=scrollbar.set)

        # Redirect stdout/stderr into the text box
        sys.stdout = TextRedirector(self.status_box)
        sys.stderr = TextRedirector(self.status_box)

    # ------------------------------------------------------------------------------------------------
    # Helper: Get Default Month Period
    # ------------------------------------------------------------------------------------------------
    def get_default_month_period(self):
        """Determine default reporting period (previous or current month)."""
        today = dt.date.today()
        first_of_this_month = today.replace(day=1)
        last_month_end = first_of_this_month - dt.timedelta(days=1)
        first_of_last_month = last_month_end.replace(day=1)

        # If within 9 days of month-end, use previous month; else current
        if today <= (last_month_end + dt.timedelta(days=9)):
            default_month = first_of_last_month
        else:
            default_month = first_of_this_month

        start_date = default_month.strftime("%Y-%m-01")
        end_date = (default_month.replace(day=calendar.monthrange(default_month.year, default_month.month)[1])
                    .strftime("%Y-%m-%d"))
        return default_month, start_date, end_date

    # ------------------------------------------------------------------------------------------------
    # Helper: Log to Status Box
    # ------------------------------------------------------------------------------------------------
    def log(self, message):
        """Append a message to the status box with auto-scroll."""
        timestamp = dt.datetime.now().strftime("%H:%M:%S")
        self.status_box.config(state="normal")
        self.status_box.insert("end", f"[{timestamp}] {message}\n")
        self.status_box.see("end")
        self.status_box.config(state="disabled")
        self.update_idletasks()

    # ------------------------------------------------------------------------------------------------
    # Run Extraction Process
    # ------------------------------------------------------------------------------------------------
    def run_extraction(self):
        """Run the DWH extraction process with selected options."""
        self.run_button.config(state="disabled")
        self.log("Starting DWH extraction...")

        # Resolve selected email
        email_choice = self.email_var.get()
        if email_choice == "gerry":
            selected_email = "gerry.pidgeon@gopuff.com"
        elif email_choice == "dimitrios":
            selected_email = "dimitrios.kakkavas@gopuff.com"
        else:
            selected_email = self.custom_email_var.get().strip()
            if not selected_email:
                messagebox.showerror("Error", "Please enter a custom email address.")
                self.run_button.config(state="normal")
                return

        os.environ["SNOWFLAKE_USER"] = selected_email
        self.log(f"Using Snowflake user: {selected_email}")

        # Resolve reporting period
        override = self.month_override_var.get().strip()
        if override:
            try:
                year, month = map(int, override.split("-"))
                start_date = f"{year}-{month:02d}-01"
                last_day = calendar.monthrange(year, month)[1]
                end_date = f"{year}-{month:02d}-{last_day:02d}"
                self.log(f"Overriding reporting period → {start_date} → {end_date}")
            except Exception:
                messagebox.showerror("Error", "Invalid month format. Please use YYYY-MM.")
                self.run_button.config(state="normal")
                return
        else:
            start_date, end_date = self.start_date, self.end_date
            self.log(f"Using default reporting period → {start_date} → {end_date}")

        # 👇 Dynamically override reporting period in memory
        cfg.REPORTING_START_DATE = start_date
        cfg.REPORTING_END_DATE = end_date

        # Run main() in background thread
        threading.Thread(
            target=self._execute_main,
            args=(selected_email,),
            daemon=True,
        ).start()

    # ------------------------------------------------------------------------------------------------
    # Execute main() in Thread
    # ------------------------------------------------------------------------------------------------
    def _execute_main(self, email):
        """Run main process safely with GUI updates."""
        try:
            self.log("Connecting to Snowflake...")
            run_dwh_main()  # uses updated start/end from P07_module_configs
            self.log("✅ Extraction completed successfully.")
        except Exception as e:
            self.log(f"❌ Error: {e}")
            messagebox.showerror("Error", str(e))
        finally:
            self.run_button.config(state="normal")
