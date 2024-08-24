import pandas as pd
import tkinter as tk
from tkinter import ttk
from tkinter import filedialog

def load_csv(file_path):
    df = pd.read_csv(file_path, index_col=0)
    return df

def main():
    # Initialize the main window
    root = tk.Tk()
    root.title("CSV Viewer")
    root.geometry("800x600")

    # Create a frame to hold the widgets
    frame = ttk.Frame(root, padding="10")
    frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))

    frame.columnconfigure(0, weight=1)
    frame.columnconfigure(1, weight=1)
    frame.columnconfigure(2, weight=1)
    frame.columnconfigure(3, weight=1)
    frame.rowconfigure(1, weight=1)

    # Create a text widget to display the summary
    summary_text = tk.Text(frame, wrap=tk.WORD)
    summary_text.grid(row=1, column=0, columnspan=4, sticky=(tk.W, tk.E, tk.N, tk.S), rowspan=5)

    scroll_y = ttk.Scrollbar(frame, orient=tk.VERTICAL, command=summary_text.yview)
    scroll_y.grid(row=1, column=4, sticky=(tk.N, tk.S))
    summary_text["yscrollcommand"] = scroll_y.set

    scroll_x = ttk.Scrollbar(frame, orient=tk.HORIZONTAL, command=summary_text.xview)
    scroll_x.grid(row=6, column=0, columnspan=4, sticky=(tk.W, tk.E))
    summary_text["xscrollcommand"] = scroll_x.set

    # Create widgets for filtering data
    column_filter_label = ttk.Label(frame, text="Select columns:")
    column_filter_label.grid(row=0, column=0, sticky=tk.W)

    column_filter = ttk.Combobox(frame, values=[], state="readonly")
    column_filter.grid(row=0, column=1, sticky=(tk.W, tk.E))

    index_filter_label = ttk.Label(frame, text="Filter by index:")
    index_filter_label.grid(row=0, column=2, sticky=tk.W)

    index_filter = ttk.Entry(frame)
    index_filter.grid(row=0, column=3, sticky=(tk.W, tk.E))

    value_filter_label = ttk.Label(frame, text="Filter by value:")
    value_filter_label.grid(row=7, column=0, sticky=tk.W)

    value_filter = ttk.Entry(frame)
    value_filter.grid(row=7, column=1, sticky=(tk.W, tk.E))

    def apply_filters():
        try:
            if not df.empty:
                filtered_df = df.copy()
                column = column_filter.get()
                index_value = index_filter.get()
                value = value_filter.get()

                if column:
                    filtered_df = filtered_df[[column]]

                if column and value:
                    try:
                        numeric_value = float(value)
                        filtered_df = filtered_df[filtered_df[column].astype(float) == numeric_value]
                    except ValueError:
                        filtered_df = filtered_df[filtered_df[column] == value]

                if index_value:
                    try:
                        filtered_df = filtered_df.loc[[index_value]]
                    except KeyError:
                        filtered_df = pd.DataFrame(columns=filtered_df.columns)

                update_summary(filtered_df)
        except Exception as e:
            summary_text.insert(tk.END, f"Error: {str(e)}")

    # Create a button to apply filters
    apply_button = ttk.Button(frame, text="Apply Filters", command=apply_filters)
    apply_button.grid(row=7, column=2, sticky=tk.W)

    def open_csv():
        file_path = filedialog.askopenfilename(filetypes=[("CSV files", "*.csv")])
        if file_path:
            global df
            df = load_csv(file_path)
            column_filter["values"] = list(df.columns)
            update_summary(df)

    open_button = ttk.Button(frame, text="Open CSV", command=open_csv)
    open_button.grid(row=7, column=3, sticky=tk.W)

    def update_summary(data):
        summary_text.delete(1.0, tk.END)
        summary_text.insert(tk.END, data.to_string())

    df = pd.DataFrame()

    def on_closing():
        root.destroy()

    root.protocol("WM_DELETE_WINDOW", on_closing)

    root.mainloop()

if __name__ == "__main__":
    main()
