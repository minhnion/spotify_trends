#!/bin/bash
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"


PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

PYTHON_SCRIPT="spark_jobs/loading/populate_database.py"

echo "=================================================="
echo "STARTING DATABASE POPULATION JOB"
echo "=================================================="
echo "Script Dir:  $SCRIPT_DIR"
echo "Project Root: $PROJECT_ROOT"

cd "$PROJECT_ROOT"

if [ -d "venv" ]; then
    echo "Activating virtual environment (venv)..."
    source venv/bin/activate
else
    echo "WARNING: Không tìm thấy thư mục 'venv' tại $PROJECT_ROOT/venv."
    echo "   Đang chạy với Python hệ thống."
fi

export PYTHONPATH="$PROJECT_ROOT:$PYTHONPATH"

if [ ! -d "jars" ]; then
    echo "ERROR: Không tìm thấy thư mục 'jars' tại $PROJECT_ROOT/jars"
    echo "   Vui lòng chạy script download_jars.sh ở thư mục gốc trước."
    exit 1
fi

if [ ! -f "$PYTHON_SCRIPT" ]; then
    echo "ERROR: Không tìm thấy file script tại: $PROJECT_ROOT/$PYTHON_SCRIPT"
    exit 1
fi

# 8. Chạy
echo "--------------------------------------------------"
echo "RUNNING: python $PYTHON_SCRIPT"
echo "--------------------------------------------------"

python "$PYTHON_SCRIPT"

EXIT_CODE=$?
if [ $EXIT_CODE -eq 0 ]; then
    echo "SUCCESS"
else
    echo "FAILED (Exit Code: $EXIT_CODE)"
fi

exit $EXIT_CODE