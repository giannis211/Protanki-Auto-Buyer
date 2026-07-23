#  Protanki Mine Autobuyer

An automated desktop application built with Python and Tkinter to automatically purchase mine supplies on ProTanki servers. Built using the **[ProboTanki-Lib](https://github.com/Teinc3/ProboTanki-Lib)** protocol library.

---

## Usage

* **Configurable Auto-Buy:** Set a custom count per purchase and an interval (down to 1 ms, though **recommended interval is 100 ms** so you don't run into server rate-limiting or connection issues).
* **Standalone Executable:** No Python installation required if using the prebuilt Windows release.

### Downloads

* **[Download Prebuilt App (.exe)]([https://github.com/giannis211/Protanki-Auto-Buyer/releases/download/v1/MineBuyer.exe])**
* **[ProboTanki-Lib Source Repository](https://github.com/Teinc3/ProboTanki-Lib)**


## Build Instructions

If you prefer to compile the application from source into a standalone executable yourself using PyInstaller, make sure you have Python installed along with `pbtlib` (`probotanki-lib`) and `pyinstaller` in your environment.

Run the following command from your terminal inside the project folder:

pyinstaller --onefile --noconsole --name "MineBuyer" --icon "assets\icon.ico" --add-data "assets;assets" --collect-all pbtlib main.py

---

## How It Works

1. The app opens a new TCP connection with the ProTanki server.
2. It logs into your account using your credentials.
3. It loads your crystal balance, loads the garage, and retrieves the price of 1 mine.
4. Once loaded, you can start the auto-buy process.

> [!WARNING]
> **Important:** Do **not** log into your account in-game while the autobuyer is working. Either stop the autobuyer or let it finish, close the app, and then open the ProTanki client to see the results.
