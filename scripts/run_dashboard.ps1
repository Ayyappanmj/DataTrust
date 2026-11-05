<# .SYNOPSIS Launch the Streamlit dashboard at http://localhost:8501 . #>
. "$PSScriptRoot\_common.ps1"
& (Get-VenvPython) -m streamlit run app/dashboard.py
