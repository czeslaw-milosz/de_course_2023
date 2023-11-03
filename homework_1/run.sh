python3.10 -m venv tmp && source ./tmp/bin/activate && pip install -r requirements.txt;
make setup; sleep 5;
python ./run_pipeline.py;
source deactivate;