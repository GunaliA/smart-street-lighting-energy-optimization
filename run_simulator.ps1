$ErrorActionPreference = "Stop"
.\.venv\Scripts\Activate.ps1
python src\simulator\simulator.py
python src\analytics\validate_data.py
