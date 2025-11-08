## Training routine

The toy linear-regression example lives in `src/train.py`. Run it with uv so
dependencies come from the managed environment:

```bash
uv run python src/train.py
```

It will train on `data.csv`, print validation metrics, and can be imported for
`train_model`/`validate_model` helpers inside other scripts.

## MLflow orchestration

Minimal tracking lives in `orchestration/run.py`. It wraps the training helper,
logs the dataset path, metrics, and the fitted sklearn model to MLflow. Runs are
stored in `mlflow.db` via SQLite so we avoid the deprecated file backend warning
and we log both a model signature and input example. Execute it with:

```bash
uv run python orchestration/run.py
```

The same module exposes helpers to bring models back without digging through
artifacts:

```python
from orchestration.run import load_latest_model

model, run_id = load_latest_model()
print(run_id, model.predict([[11]]))
```
