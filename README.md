# Excel Report Extractor

A Python-based tool that extracts structured data from Excel files using multi-threading for efficient processing.

## 🔧 Setup Instructions

1. **Clone the repository**

    ```bash
      git clone git clone https://github.com/kenulh3rath/Kazakhstan-Crime-Data-Data-Extractor.git
    ```

2. **Navigate to the project directory**

    ```bash
      cd Kazakhstan-Crime-Data-Data-Extractor
    ```

3. **Create Virtual Enviroment**
    ```bash
        python -m venv venv
    ```

4. **Activate the virtual environment**
    ```bash
     venv\Scripts\activate
    ```

5. **Install the required packages**

    ```bash
      pip install -r requirements.txt
    ```

6. **Change directory name in main.py**
- Get the file path for the root directory of the project.
- Change the directory name in `main.py` (line 35) to match your project structure.

    ```
    DIR = "D:\Github\Kazakhstan-Crime-Data-Data-Extractor"
    ```
- Make sure all the slashes are forward slashes (`/`) to avoid path issues.'


7. **Run the script**

    ```bash
      python main.py
    ```

# 🚀 What It Does
- Automatically scans all Excel files in the data/ folder.
- Each file is processed in a separate thread, allowing all files to be handled concurrently.
- Extracted data is stored in a dataclass dictionary.
- Once all threads are complete:
  - The program organizes the data by year and month.
  - Final results are saved to an Excel file in the reports/ folder.