# spark/scripts/start-jupyter.sh
#!/bin/bash
jupyter notebook --ip=0.0.0.0 \
                --port=3002 \
                --allow-root \
                --NotebookApp.token='' \
                --NotebookApp.password='' \
                --notebook-dir=/opt/spark/notebooks